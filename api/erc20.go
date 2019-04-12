package api

import (
	"fmt"
	"net/http"

	"github.com/julienschmidt/httprouter"

	erc20ext "github.com/threefoldtech/rivine-extension-erc20"
	erc20types "github.com/threefoldtech/rivine-extension-erc20/types"
	"github.com/threefoldtech/rivine/pkg/api"
	types "github.com/threefoldtech/rivine/types"
)

type (
	// PluginGetERC20RelatedAddress contains the requested ERC20-related addresses.
	PluginGetERC20RelatedAddress struct {
		TFTAddress   types.UnlockHash        `json:"tftaddress"`
		ERC20Address erc20types.ERC20Address `json:"erc20address"`
	}
	// PluginGetERC20TransactionID contains the requested info found for the given ERC20 TransactionID.
	PluginGetERC20TransactionID struct {
		ERC20TransaxtionID   erc20types.ERC20Hash `json:"er20txid"`
		TfchainTransactionID types.TransactionID  `json:"tfttxid"`
	}
)

// RegisterERC20HTTPHandlers registers the (tfchain-specific) handlers for all ERC20 HTTP endpoints.
func RegisterERC20HTTPHandlers(router httprouter.Router, erc20plugin *erc20ext.Plugin) {
	if erc20plugin == nil {
		panic("no erc20plugin given")
	}

	router.GET("/consensus/erc20/addresses/:address", NewPluginGetERC20RelatedAddressHandler(erc20plugin))
	router.GET("/consensus/erc20/transactions/:txid", NewPluginGetERC20TransactionID(erc20plugin))
}

// NewPluginGetERC20RelatedAddressHandler creates a handler to handle the API calls to /transactiondb/erc20/addresses/:address.
func NewPluginGetERC20RelatedAddressHandler(plugin *erc20ext.Plugin) httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
		addressStr := ps.ByName("address")

		var (
			err   error
			found bool
			resp  PluginGetERC20RelatedAddress
		)
		if len(addressStr) == erc20types.ERC20AddressLength*2 {
			err = resp.ERC20Address.LoadString(addressStr)
			if err != nil {
				api.WriteError(w, api.Error{Message: fmt.Sprintf("invalid ERC20 address given: %v", err)}, http.StatusBadRequest)
				return
			}
			resp.TFTAddress, found, err = plugin.GetTFTAddressForERC20Address(resp.ERC20Address)
			if err != nil {
				api.WriteError(w, api.Error{Message: fmt.Sprintf("error while fetching TFT Address: %v", err)}, http.StatusInternalServerError)
				return
			}
			if !found {
				api.WriteError(w, api.Error{Message: "error while fetching TFT Address: address not found"}, http.StatusNoContent)
				return
			}
		} else {
			err = resp.TFTAddress.LoadString(addressStr)
			if err != nil {
				api.WriteError(w, api.Error{Message: fmt.Sprintf("invalid (TFT) address given: %v", err)}, http.StatusBadRequest)
				return
			}
			resp.ERC20Address, found, err = plugin.GetERC20AddressForTFTAddress(resp.TFTAddress)
			if err != nil {
				api.WriteError(w, api.Error{Message: fmt.Sprintf("error while fetching ERC20 Address: %v", err)}, http.StatusInternalServerError)
				return
			}
			if !found {
				api.WriteError(w, api.Error{Message: "error while fetching ERC20 Address: address not found"}, http.StatusNoContent)
				return
			}
		}
		api.WriteJSON(w, resp)
	}
}

// NewPluginGetERC20TransactionID creates a handler to handle the API calls to /transactiondb/erc20/transactions/:txid.
func NewPluginGetERC20TransactionID(plugin *erc20ext.Plugin) httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
		txidStr := ps.ByName("txid")
		var txid erc20types.ERC20Hash
		err := txid.LoadString(txidStr)
		if err != nil {
			api.WriteError(w, api.Error{Message: fmt.Sprintf("invalid ERC20 TransactionID given: %v", err)}, http.StatusBadRequest)
			return
		}

		tfttxid, found, err := plugin.GetTFTTransactionIDForERC20TransactionID(txid)
		if err != nil {
			api.WriteError(w, api.Error{Message: fmt.Sprintf("error while fetching info linked to ERC20 TransactionID: %v", err)}, http.StatusInternalServerError)
			return
		}
		if !found {
			api.WriteError(w, api.Error{Message: "error while fetching info linked to ERC20 TransactionID: ID not found"}, http.StatusNoContent)
			return
		}

		api.WriteJSON(w, PluginGetERC20TransactionID{
			ERC20TransaxtionID:   txid,
			TfchainTransactionID: tfttxid,
		})
	}
}
