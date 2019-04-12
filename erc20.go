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
	bucketERC20ToTFTAddresses = []byte("addresses_erc20_to_tft") // erc20 => TFT
	bucketTFTToERC20Addresses = []byte("addresses_tft_to_erc20") // TFT => erc20
	bucketERC20TransactionIDs = []byte("erc20_transactionids")   // stores all unique ERC20 transaction ids used for erc20=>TFT exchanges
)

type (
	// Plugin is a struct defines the minting plugin
	Plugin struct {
		storage            modules.PluginViewStorage
		unregisterCallback modules.PluginUnregisterCallback
	}

	transactionContext struct {
		BlockHeight  rivinetypes.BlockHeight
		BlockTime    rivinetypes.Timestamp
		TxSequenceID uint16
	}
)

// NewERC20Plugin creates a new erc20 plugin
func NewERC20Plugin(ERC20FeePoolAddress rivinetypes.UnlockHash, oneCoin rivinetypes.Currency, erc20TxValidator types.ERC20TransactionValidator) *Plugin {
	p := &Plugin{}
	rivinetypes.RegisterTransactionVersion(types.TransactionVersionERC20Conversion, types.ERC20ConvertTransactionController{})
	rivinetypes.RegisterTransactionVersion(types.TransactionVersionERC20CoinCreation, types.ERC20CoinCreationTransactionController{
		Registry:    p,
		OneCoin:     oneCoin,
		TxValidator: erc20TxValidator,
	})
	rivinetypes.RegisterTransactionVersion(types.TransactionVersionERC20AddressRegistration, types.ERC20AddressRegistrationTransactionController{
		Registry:             p,
		OneCoin:              oneCoin,
		BridgeFeePoolAddress: ERC20FeePoolAddress,
	})
	return p
}

// InitPlugin initializes the Bucket for the first time
func (p *Plugin) InitPlugin(metadata *persist.Metadata, bucket *bolt.Bucket, storage modules.PluginViewStorage, unregisterCallback modules.PluginUnregisterCallback) (persist.Metadata, error) {
	p.storage = storage
	p.unregisterCallback = unregisterCallback
	if metadata == nil {
		buckets := [][]byte{
			bucketERC20ToTFTAddresses,
			bucketTFTToERC20Addresses,
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
	} else if metadata.Version != pluginDBVersion || metadata.Header != pluginDBHeader {
		return persist.Metadata{}, errors.New("There is only 1 version of this plugin, version mismatch")
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
		case types.TransactionVersionERC20CoinCreation:
			err = p.applyERC20CoinCreationTx(tx, ctx, rtx)
		case types.TransactionVersionERC20AddressRegistration:
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
		case types.TransactionVersionERC20CoinCreation:
			err = p.revertERC20CoinCreationTx(tx, ctx, rtx)
		case types.TransactionVersionERC20AddressRegistration:
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
	etartx, err := types.ERC20AddressRegistrationTransactionFromTransaction(*rtx)
	if err != nil {
		return fmt.Errorf("unexpected error while unpacking the ERC20 Address Registration tx type: %v", err)
	}

	tftaddr := rivinetypes.NewPubKeyUnlockHash(etartx.PublicKey)
	erc20addr := types.ERC20AddressFromUnlockHash(tftaddr)

	return applyERC20AddressMapping(tx, tftaddr, erc20addr)
}

func (p *Plugin) revertERC20AddressRegistrationTx(tx *bolt.Tx, ctx transactionContext, rtx *rivinetypes.Transaction) error {
	etartx, err := types.ERC20AddressRegistrationTransactionFromTransaction(*rtx)
	if err != nil {
		return fmt.Errorf("unexpected error while unpacking the ERC20 Address Registration tx type: %v", err)
	}

	tftaddr := rivinetypes.NewPubKeyUnlockHash(etartx.PublicKey)
	erc20addr := types.ERC20AddressFromUnlockHash(tftaddr)

	return revertERC20AddressMapping(tx, tftaddr, erc20addr)
}

func (p *Plugin) applyERC20CoinCreationTx(tx *bolt.Tx, ctx transactionContext, rtx *rivinetypes.Transaction) error {
	etcctx, err := types.ERC20CoinCreationTransactionFromTransaction(*rtx)
	if err != nil {
		return fmt.Errorf("unexpected error while unpacking the ERC20 Coin Creation Tx type: %v", err)
	}
	return applyERC20TransactionID(tx, etcctx.TransactionID, rtx.ID())
}

func (p *Plugin) revertERC20CoinCreationTx(tx *bolt.Tx, ctx transactionContext, rtx *rivinetypes.Transaction) error {
	etcctx, err := types.ERC20CoinCreationTransactionFromTransaction(*rtx)
	if err != nil {
		return fmt.Errorf("unexpected error while unpacking the ERC20 Coin Creation Tx type: %v", err)
	}
	return revertERC20TransactionID(tx, etcctx.TransactionID)
}

// GetERC20AddressForTFTAddress returns the mapped ERC20 address for the given TFT Address,
// if the TFT Address has registered an ERC20 address explicitly.
func (p *Plugin) GetERC20AddressForTFTAddress(uh rivinetypes.UnlockHash) (addr types.ERC20Address, found bool, err error) {
	err = p.storage.View(func(bucket *bolt.Bucket) error {
		tx := bucket.Tx()
		addr, found, err = getERC20AddressForTFTAddress(tx, uh)
		if err != nil {
			return err
		}
		return nil
	})
	return
}

// GetTFTAddressForERC20Address returns the mapped TFT address for the given ERC20 Address,
// if the TFT Address has registered an ERC20 address explicitly.
func (p *Plugin) GetTFTAddressForERC20Address(addr types.ERC20Address) (uh rivinetypes.UnlockHash, found bool, err error) {
	err = p.storage.View(func(bucket *bolt.Bucket) error {
		tx := bucket.Tx()
		uh, found, err = getTFTAddressForERC20Address(tx, addr)
		if err != nil {
			return err
		}
		return nil
	})
	return
}

// GetTFTTransactionIDForERC20TransactionID returns the mapped TFT TransactionID for the given ERC20 TransactionID,
// if the ERC20 TransactionID has been used to fund an ERC20 CoinCreation Tx and has been registered as such, a nil TransactionID is returned otherwise.
func (p *Plugin) GetTFTTransactionIDForERC20TransactionID(id types.ERC20Hash) (txid rivinetypes.TransactionID, found bool, err error) {
	err = p.storage.View(func(bucket *bolt.Bucket) error {
		tx := bucket.Tx()
		txid, found, err = getTfchainTransactionIDForERC20TransactionID(tx, id)
		if err != nil {
			return err
		}
		return nil
	})
	return
}

func applyERC20AddressMapping(tx *bolt.Tx, tftaddr rivinetypes.UnlockHash, erc20addr types.ERC20Address) error {
	btft, berc20 := rivbin.Marshal(tftaddr), rivbin.Marshal(erc20addr)

	// store ERC20->TFT mapping
	bucket := tx.Bucket(bucketERC20ToTFTAddresses)
	if bucket == nil {
		return errors.New("corrupt transaction DB: ERC20->TFT bucket does not exist")
	}
	err := bucket.Put(berc20, btft)
	if err != nil {
		return fmt.Errorf("error while storing ERC20->TFT address mapping: %v", err)
	}

	// store TFT->ERC20 mapping
	bucket = tx.Bucket(bucketTFTToERC20Addresses)
	if bucket == nil {
		return errors.New("corrupt transaction DB: TFT->ERC20 bucket does not exist")
	}
	err = bucket.Put(btft, berc20)
	if err != nil {
		return fmt.Errorf("error while storing TFT->ERC20 address mapping: %v", err)
	}

	// done
	return nil
}
func revertERC20AddressMapping(tx *bolt.Tx, tftaddr rivinetypes.UnlockHash, erc20addr types.ERC20Address) error {
	btft, berc20 := rivbin.Marshal(tftaddr), rivbin.Marshal(erc20addr)

	// delete ERC20->TFT mapping
	bucket := tx.Bucket(bucketERC20ToTFTAddresses)
	if bucket == nil {
		return errors.New("corrupt transaction DB: ERC20->TFT bucket does not exist")
	}
	err := bucket.Delete(berc20)
	if err != nil {
		return fmt.Errorf("error while deleting ERC20->TFT address mapping: %v", err)
	}

	// delete TFT->ERC20 mapping
	bucket = tx.Bucket(bucketTFTToERC20Addresses)
	if bucket == nil {
		return errors.New("corrupt transaction DB: TFT->ERC20 bucket does not exist")
	}
	err = bucket.Delete(btft)
	if err != nil {
		return fmt.Errorf("error while deleting TFT->ERC20 address mapping: %v", err)
	}

	// done
	return nil
}

func getERC20AddressForTFTAddress(tx *bolt.Tx, uh rivinetypes.UnlockHash) (types.ERC20Address, bool, error) {
	bucket := tx.Bucket(bucketTFTToERC20Addresses)
	if bucket == nil {
		return types.ERC20Address{}, false, errors.New("corrupt transaction DB: TFT->ERC20 bucket does not exist")
	}
	b := bucket.Get(rivbin.Marshal(uh))
	if len(b) == 0 {
		return types.ERC20Address{}, false, nil
	}
	var addr types.ERC20Address
	err := rivbin.Unmarshal(b, &addr)
	if err != nil {
		return types.ERC20Address{}, false, fmt.Errorf("failed to fetch ERC20 Address for TFT address %v: %v", uh, err)
	}
	return addr, true, nil
}

func getTFTAddressForERC20Address(tx *bolt.Tx, addr types.ERC20Address) (rivinetypes.UnlockHash, bool, error) {
	bucket := tx.Bucket(bucketERC20ToTFTAddresses)
	if bucket == nil {
		return rivinetypes.UnlockHash{}, false, errors.New("corrupt transaction DB: ERC20->TFT bucket does not exist")
	}
	b := bucket.Get(rivbin.Marshal(addr))
	if len(b) == 0 {
		return rivinetypes.UnlockHash{}, false, nil
	}
	var uh rivinetypes.UnlockHash
	err := rivbin.Unmarshal(b, &uh)
	if err != nil {
		return rivinetypes.UnlockHash{}, false, fmt.Errorf("failed to fetch TFT Address for ERC20 address %v: %v", addr, err)
	}
	return uh, true, nil
}

func applyERC20TransactionID(tx *bolt.Tx, erc20id types.ERC20Hash, tftid rivinetypes.TransactionID) error {
	bucket := tx.Bucket(bucketERC20TransactionIDs)
	if bucket == nil {
		return errors.New("corrupt transaction DB: ERC20 TransactionIDs bucket does not exist")
	}
	err := bucket.Put(rivbin.Marshal(erc20id), rivbin.Marshal(tftid))
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

func getTfchainTransactionIDForERC20TransactionID(tx *bolt.Tx, id types.ERC20Hash) (rivinetypes.TransactionID, bool, error) {
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
		return rivinetypes.TransactionID{}, false, fmt.Errorf("corrupt transaction DB: invalid tfchain TransactionID fetched for ERC20 TxID %v: %v", id, err)
	}
	return txid, true, nil
}
