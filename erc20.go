package minting

import (
	"errors"
	"fmt"

	"github.com/threefoldtech/rivine-extension-erc20/types"
	"github.com/threefoldtech/rivine/modules"
	"github.com/threefoldtech/rivine/persist"
	"github.com/threefoldtech/rivine/pkg/encoding/rivbin"
	rivinetypes "github.com/threefoldtech/rivine/types"

	bolt "github.com/rivine/bbolt"
)

const (
	pluginDBVersion = "1.0.0.0"
	pluginDBHeader  = "erc20Plugin"
)

var (
	// buckets for the ERC20-bridge feature
	bucketERC20ToRivineAddresses = []byte("addresses_erc20_to_riv") // erc20 => Rivine
	bucketRivineToERC20Addresses = []byte("addresses_riv_to_erc20") // Rivine => erc20
	bucketERC20TransactionIDs    = []byte("erc20_transactionids")   // stores all unique ERC20 transaction ids used for erc20=>Rivine exchanges
)

type (
	// Plugin is a struct defines the ERC20 extension plugin
	Plugin struct {
		storage            modules.PluginViewStorage
		unregisterCallback modules.PluginUnregisterCallback

		coinCreationTxVersion        rivinetypes.TransactionVersion
		addressRegistrationTxVersion rivinetypes.TransactionVersion
	}

	// PluginConfig is used when creating the ERC20 extension plugin,
	// all parameters are required
	PluginConfig struct {
		ERC20FeePoolAddress  rivinetypes.UnlockHash
		OneCoin              rivinetypes.Currency
		TransactionValidator types.ERC20TransactionValidator

		// transaction version
		ConvertTransactionVersion             rivinetypes.TransactionVersion
		CoinCreationTransactionVersion        rivinetypes.TransactionVersion
		AddressRegistrationTransactionVersion rivinetypes.TransactionVersion
	}

	transactionContext struct {
		BlockHeight  rivinetypes.BlockHeight
		BlockTime    rivinetypes.Timestamp
		TxSequenceID uint16
	}
)

func (cfg *PluginConfig) validate() error {
	if cfg.ERC20FeePoolAddress.Cmp(rivinetypes.NilUnlockHash) == 0 {
		return errors.New("required config parameter ERC20FeePoolAddress is not defined (nil unlockhash is not allowed)")
	}
	if cfg.OneCoin.IsZero() {
		return errors.New("required config parameter OneCoin is not defined (0 is not allowed)")
	}
	if cfg.TransactionValidator == nil {
		return errors.New("required config parameter TransactionValidator is not defined (nil is not allowed)")
	}
	if cfg.ConvertTransactionVersion == 0 {
		return errors.New("required config parameter ConvertTransactionVersion is not defined (0 is not allowed)")
	}
	if cfg.CoinCreationTransactionVersion == 0 {
		return errors.New("required config parameter CoinCreationTransactionVersion is not defined (0 is not allowed)")
	}
	if cfg.AddressRegistrationTransactionVersion == 0 {
		return errors.New("required config parameter AddressRegistrationTransactionVersion is not defined (0 is not allowed)")
	}
	return nil
}

// NewPlugin creates a new ERC20-Extension plugin
func NewPlugin(cfg PluginConfig) (*Plugin, error) {
	err := cfg.validate()
	if err != nil {
		return nil, err
	}
	p := &Plugin{
		coinCreationTxVersion:        cfg.CoinCreationTransactionVersion,
		addressRegistrationTxVersion: cfg.AddressRegistrationTransactionVersion,
	}
	rivinetypes.RegisterTransactionVersion(cfg.ConvertTransactionVersion, types.ERC20ConvertTransactionController{TransactionVersion: cfg.ConvertTransactionVersion})
	rivinetypes.RegisterTransactionVersion(cfg.CoinCreationTransactionVersion, types.ERC20CoinCreationTransactionController{
		Registry:           p,
		OneCoin:            cfg.OneCoin,
		TxValidator:        cfg.TransactionValidator,
		TransactionVersion: cfg.CoinCreationTransactionVersion,
	})
	rivinetypes.RegisterTransactionVersion(cfg.AddressRegistrationTransactionVersion, types.ERC20AddressRegistrationTransactionController{
		Registry:             p,
		OneCoin:              cfg.OneCoin,
		BridgeFeePoolAddress: cfg.ERC20FeePoolAddress,
		TransactionVersion:   cfg.AddressRegistrationTransactionVersion,
	})
	return p, nil
}

// InitPlugin initializes the Bucket for the first time
func (p *Plugin) InitPlugin(metadata *persist.Metadata, bucket *bolt.Bucket, storage modules.PluginViewStorage, unregisterCallback modules.PluginUnregisterCallback) (persist.Metadata, error) {
	p.storage = storage
	p.unregisterCallback = unregisterCallback
	if metadata == nil {
		buckets := [][]byte{
			bucketERC20ToRivineAddresses,
			bucketRivineToERC20Addresses,
			bucketERC20TransactionIDs,
		}
		var err error
		for _, bckt := range buckets {
			_, err = bucket.CreateBucketIfNotExists(bckt)
			if err != nil {
				return persist.Metadata{}, err
			}
		}

		metadata = &persist.Metadata{
			Version: pluginDBVersion,
			Header:  pluginDBHeader,
		}
	} else if metadata.Version != pluginDBVersion {
		return persist.Metadata{}, errors.New("There is only 1 version of this (ERC20) plugin, version mismatch")
	} else if metadata.Header != pluginDBHeader {
		return persist.Metadata{}, errors.New("wrong (ERC20) plugin DB header")
	}
	return *metadata, nil
}

// ApplyBlock applies a block's erc20 transaction to their buckets.
func (p *Plugin) ApplyBlock(block rivinetypes.Block, height rivinetypes.BlockHeight, bucket *persist.LazyBoltBucket) error {
	tx, err := bucket.Tx()
	if err != nil {
		return errors.New("erc20 bucket does not exist")
	}
	for i := range block.Transactions {
		rtx := &block.Transactions[i]
		if rtx.Version == rivinetypes.TransactionVersionOne {
			continue // ignore most common Tx
		}
		ctx := transactionContext{
			BlockHeight:  height,
			BlockTime:    block.Timestamp,
			TxSequenceID: uint16(i),
		}
		// check the version and handle the ones we care about
		switch rtx.Version {
		case p.coinCreationTxVersion:
			err = p.applyERC20CoinCreationTx(tx, ctx, rtx)
		case p.addressRegistrationTxVersion:
			err = p.applyERC20AddressRegistrationTx(tx, ctx, rtx)
		}
	}
	return nil
}

// RevertBlock reverts a block's erc20 transaction from their buckets
func (p *Plugin) RevertBlock(block rivinetypes.Block, height rivinetypes.BlockHeight, bucket *persist.LazyBoltBucket) error {
	tx, err := bucket.Tx()
	if err != nil {
		return errors.New("erc20 bucket does not exist")
	}
	// collect all one-per-block mint conditions
	for i := range block.Transactions {
		rtx := &block.Transactions[i]
		if rtx.Version == rivinetypes.TransactionVersionOne {
			continue // ignore most common Tx
		}
		ctx := transactionContext{
			BlockHeight:  height,
			BlockTime:    block.Timestamp,
			TxSequenceID: uint16(i),
		}

		// check the version and handle the ones we care about
		switch rtx.Version {
		case p.coinCreationTxVersion:
			err = p.revertERC20CoinCreationTx(tx, ctx, rtx)
		case p.addressRegistrationTxVersion:
			err = p.revertERC20AddressRegistrationTx(tx, ctx, rtx)
		}
	}
	return nil
}

// Close unregisters the plugin from the consensus
func (p *Plugin) Close() error {
	p.unregisterCallback(p)
	return p.storage.Close()
}

func (p *Plugin) applyERC20AddressRegistrationTx(tx *bolt.Tx, ctx transactionContext, rtx *rivinetypes.Transaction) error {
	etartx, err := types.ERC20AddressRegistrationTransactionFromTransaction(*rtx, p.addressRegistrationTxVersion)
	if err != nil {
		return fmt.Errorf("unexpected error while unpacking the ERC20 Address Registration tx type: %v", err)
	}

	addr := rivinetypes.NewPubKeyUnlockHash(etartx.PublicKey)
	erc20addr := types.ERC20AddressFromUnlockHash(addr)

	return applyERC20AddressMapping(tx, addr, erc20addr)
}

func (p *Plugin) revertERC20AddressRegistrationTx(tx *bolt.Tx, ctx transactionContext, rtx *rivinetypes.Transaction) error {
	etartx, err := types.ERC20AddressRegistrationTransactionFromTransaction(*rtx, p.addressRegistrationTxVersion)
	if err != nil {
		return fmt.Errorf("unexpected error while unpacking the ERC20 Address Registration tx type: %v", err)
	}

	addr := rivinetypes.NewPubKeyUnlockHash(etartx.PublicKey)
	erc20addr := types.ERC20AddressFromUnlockHash(addr)

	return revertERC20AddressMapping(tx, addr, erc20addr)
}

func (p *Plugin) applyERC20CoinCreationTx(tx *bolt.Tx, ctx transactionContext, rtx *rivinetypes.Transaction) error {
	etcctx, err := types.ERC20CoinCreationTransactionFromTransaction(*rtx, p.coinCreationTxVersion)
	if err != nil {
		return fmt.Errorf("unexpected error while unpacking the ERC20 Coin Creation Tx type: %v", err)
	}
	return applyERC20TransactionID(tx, etcctx.TransactionID, rtx.ID())
}

func (p *Plugin) revertERC20CoinCreationTx(tx *bolt.Tx, ctx transactionContext, rtx *rivinetypes.Transaction) error {
	etcctx, err := types.ERC20CoinCreationTransactionFromTransaction(*rtx, p.coinCreationTxVersion)
	if err != nil {
		return fmt.Errorf("unexpected error while unpacking the ERC20 Coin Creation Tx type: %v", err)
	}
	return revertERC20TransactionID(tx, etcctx.TransactionID)
}

// GetERC20AddressForRivineAddress returns the mapped ERC20 address for the given (Rivine) Address,
// if the (Rivine) Address has registered an ERC20 address explicitly.
func (p *Plugin) GetERC20AddressForRivineAddress(uh rivinetypes.UnlockHash) (addr types.ERC20Address, found bool, err error) {
	err = p.storage.View(func(bucket *bolt.Bucket) error {
		tx := bucket.Tx()
		addr, found, err = getERC20AddressForRivineAddress(tx, uh)
		if err != nil {
			return err
		}
		return nil
	})
	return
}

// GetRivineAddressForERC20Address returns the mapped (Rivine) address for the given ERC20 Address,
// if the (Rivine) Address has registered an ERC20 address explicitly.
func (p *Plugin) GetRivineAddressForERC20Address(addr types.ERC20Address) (uh rivinetypes.UnlockHash, found bool, err error) {
	err = p.storage.View(func(bucket *bolt.Bucket) error {
		tx := bucket.Tx()
		uh, found, err = getRivineAddressForERC20Address(tx, addr)
		if err != nil {
			return err
		}
		return nil
	})
	return
}

// GetTransactionIDForERC20TransactionID returns the mapped (Rivine) TransactionID for the given ERC20 TransactionID,
// if the ERC20 TransactionID has been used to fund an ERC20 CoinCreation Tx and has been registered as such, a nil TransactionID is returned otherwise.
func (p *Plugin) GetTransactionIDForERC20TransactionID(id types.ERC20Hash) (txid rivinetypes.TransactionID, found bool, err error) {
	err = p.storage.View(func(bucket *bolt.Bucket) error {
		tx := bucket.Tx()
		txid, found, err = getTransactionIDForERC20TransactionID(tx, id)
		if err != nil {
			return err
		}
		return nil
	})
	return
}

func applyERC20AddressMapping(tx *bolt.Tx, addr rivinetypes.UnlockHash, erc20addr types.ERC20Address) error {
	briv, berc20 := rivbin.Marshal(addr), rivbin.Marshal(erc20addr)

	// store ERC20->Rivine mapping
	bucket := tx.Bucket(bucketERC20ToRivineAddresses)
	if bucket == nil {
		return errors.New("corrupt transaction DB: ERC20->Rivine bucket does not exist")
	}
	err := bucket.Put(berc20, briv)
	if err != nil {
		return fmt.Errorf("error while storing ERC20->Rivine address mapping: %v", err)
	}

	// store Rivine->ERC20 mapping
	bucket = tx.Bucket(bucketRivineToERC20Addresses)
	if bucket == nil {
		return errors.New("corrupt transaction DB: Rivine->ERC20 bucket does not exist")
	}
	err = bucket.Put(briv, berc20)
	if err != nil {
		return fmt.Errorf("error while storing Rivine->ERC20 address mapping: %v", err)
	}

	// done
	return nil
}
func revertERC20AddressMapping(tx *bolt.Tx, addr rivinetypes.UnlockHash, erc20addr types.ERC20Address) error {
	briv, berc20 := rivbin.Marshal(addr), rivbin.Marshal(erc20addr)

	// delete ERC20->Rivine mapping
	bucket := tx.Bucket(bucketERC20ToRivineAddresses)
	if bucket == nil {
		return errors.New("corrupt transaction DB: ERC20->Rivine bucket does not exist")
	}
	err := bucket.Delete(berc20)
	if err != nil {
		return fmt.Errorf("error while deleting ERC20->Rivine address mapping: %v", err)
	}

	// delete Rivine->ERC20 mapping
	bucket = tx.Bucket(bucketRivineToERC20Addresses)
	if bucket == nil {
		return errors.New("corrupt transaction DB: Rivine->ERC20 bucket does not exist")
	}
	err = bucket.Delete(briv)
	if err != nil {
		return fmt.Errorf("error while deleting Rivine->ERC20 address mapping: %v", err)
	}

	// done
	return nil
}

func getERC20AddressForRivineAddress(tx *bolt.Tx, uh rivinetypes.UnlockHash) (types.ERC20Address, bool, error) {
	bucket := tx.Bucket(bucketRivineToERC20Addresses)
	if bucket == nil {
		return types.ERC20Address{}, false, errors.New("corrupt transaction DB: Rivine->ERC20 bucket does not exist")
	}
	b := bucket.Get(rivbin.Marshal(uh))
	if len(b) == 0 {
		return types.ERC20Address{}, false, nil
	}
	var addr types.ERC20Address
	err := rivbin.Unmarshal(b, &addr)
	if err != nil {
		return types.ERC20Address{}, false, fmt.Errorf("failed to fetch ERC20 Address for Rivine address %v: %v", uh, err)
	}
	return addr, true, nil
}

func getRivineAddressForERC20Address(tx *bolt.Tx, addr types.ERC20Address) (rivinetypes.UnlockHash, bool, error) {
	bucket := tx.Bucket(bucketERC20ToRivineAddresses)
	if bucket == nil {
		return rivinetypes.UnlockHash{}, false, errors.New("corrupt transaction DB: ERC20->Rivine bucket does not exist")
	}
	b := bucket.Get(rivbin.Marshal(addr))
	if len(b) == 0 {
		return rivinetypes.UnlockHash{}, false, nil
	}
	var uh rivinetypes.UnlockHash
	err := rivbin.Unmarshal(b, &uh)
	if err != nil {
		return rivinetypes.UnlockHash{}, false, fmt.Errorf("failed to fetch Rivine Address for ERC20 address %v: %v", addr, err)
	}
	return uh, true, nil
}

func applyERC20TransactionID(tx *bolt.Tx, erc20id types.ERC20Hash, rivid rivinetypes.TransactionID) error {
	bucket := tx.Bucket(bucketERC20TransactionIDs)
	if bucket == nil {
		return errors.New("corrupt transaction DB: ERC20 TransactionIDs bucket does not exist")
	}
	err := bucket.Put(rivbin.Marshal(erc20id), rivbin.Marshal(rivid))
	if err != nil {
		return fmt.Errorf("error while storing ERC20 TransactionID %v: %v", erc20id, err)
	}
	return nil
}

func revertERC20TransactionID(tx *bolt.Tx, id types.ERC20Hash) error {
	bucket := tx.Bucket(bucketERC20TransactionIDs)
	if bucket == nil {
		return errors.New("corrupt transaction DB: ERC20 TransactionIDs bucket does not exist")
	}
	err := bucket.Delete(rivbin.Marshal(id))
	if err != nil {
		return fmt.Errorf("error while deleting ERC20 TransactionID %v: %v", id, err)
	}
	return nil
}

func getTransactionIDForERC20TransactionID(tx *bolt.Tx, id types.ERC20Hash) (rivinetypes.TransactionID, bool, error) {
	bucket := tx.Bucket(bucketERC20TransactionIDs)
	if bucket == nil {
		return rivinetypes.TransactionID{}, false, errors.New("corrupt transaction DB: ERC20 TransactionIDs bucket does not exist")
	}
	b := bucket.Get(rivbin.Marshal(id))
	if len(b) == 0 {
		return rivinetypes.TransactionID{}, false, nil
	}
	var txid rivinetypes.TransactionID
	err := rivbin.Unmarshal(b, &txid)
	if err != nil {
		return rivinetypes.TransactionID{}, false, fmt.Errorf("corrupt transaction DB: invalid TransactionID fetched for ERC20 TxID %v: %v", id, err)
	}
	return txid, true, nil
}
