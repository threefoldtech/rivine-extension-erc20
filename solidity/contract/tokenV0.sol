pragma solidity ^0.5.0;

import "./owned_upgradeable_token_storage.sol";

// ----------------------------------------------------------------------------
// Safe maths
// ----------------------------------------------------------------------------
library SafeMath {
    function add(uint a, uint b) internal pure returns (uint c) {
        c = a + b;
        require(c >= a);
    }
    function sub(uint a, uint b) internal pure returns (uint c) {
        require(b <= a);
        c = a - b;
    }
    function mul(uint a, uint b) internal pure returns (uint c) {
        c = a * b;
        require(a == 0 || c / a == b);
    }
    function div(uint a, uint b) internal pure returns (uint c) {
        require(b > 0);
        c = a / b;
    }
}

// ----------------------------------------------------------------------------
// ERC20 Token, with the addition of symbol, name and decimals and a
// fixed supply
// ----------------------------------------------------------------------------
contract TTFT20 is OwnedUpgradeableTokenStorage {
    using SafeMath for uint;

    event Transfer(address indexed from, address indexed to, uint tokens);
    event Approval(address indexed tokenOwner, address indexed spender, uint tokens);

    // We have registered a new withdrawal address
    event RegisterWithdrawalAddress(address indexed addr);
    // Lets mint some tokens, also index the TFT tx id
    event Mint(address indexed receiver, uint tokens, string indexed txid);
    // Burn tokens in a withdrawal
    event Withdraw(address indexed from, address indexed receiver, uint tokens);

    // name, symbol and decimals getters are optional per the ERC20 spec. Normally auto generated from public variables
    // but that is obviously not going to work for us

    function name() public view returns (string memory) {
        return getName();
    }

    function symbol() public view returns (string memory) {
        return getSymbol();
    }

    function decimals() public view returns (uint8) {
        return getDecimals();
    }

    // ------------------------------------------------------------------------
    // Total supply
    // ------------------------------------------------------------------------
    function totalSupply() public view returns (uint) {
        // TODO: Is this valid for us?
        return getTotalSupply().sub(getBalance(address(0)));
    }


    // ------------------------------------------------------------------------
    // Get the token balance for account `tokenOwner`
    // ------------------------------------------------------------------------
    function balanceOf(address tokenOwner) public view returns (uint balance) {
        return getBalance(tokenOwner);
    }

    // shouldwithdraw verifies if a target has enough tokens after the edition of the new tokens
    // to pay the tft transaction fee
    function shouldWithdraw(address target, uint to_add) private view returns (bool) {
        // 0.1 TFT cost
        return getBalance(target).add(to_add) > 10**uint(getDecimals() - 1);
    }


    // ------------------------------------------------------------------------
    // Transfer the balance from token owner's account to `to` account
    // - Owner's account must have sufficient balance to transfer
    // - 0 value transfers are allowed
    // ------------------------------------------------------------------------
    function transfer(address to, uint tokens) public returns (bool success) {
        setBalance(msg.sender, getBalance(msg.sender).sub(tokens));
        if (_isWithdrawalAddress(to) && shouldWithdraw(to, tokens)) {
            emit Withdraw(msg.sender, to, tokens);
        } else {
            setBalance(to, getBalance(to).add(tokens));
            emit Transfer(msg.sender, to, tokens);
        }
        return true;
    }


    // ------------------------------------------------------------------------
    // Token owner can approve for `spender` to transferFrom(...) `tokens`
    // from the token owner's account
    //
    // https://github.com/ethereum/EIPs/blob/master/EIPS/eip-20-token-standard.md
    // recommends that there are no checks for the approval double-spend attack
    // as this should be implemented in user interfaces 
    // ------------------------------------------------------------------------
    function approve(address spender, uint tokens) public returns (bool success) {
        setAllowed(msg.sender, spender, tokens);
        emit Approval(msg.sender, spender, tokens);
        return true;
    }


    // ------------------------------------------------------------------------
    // Transfer `tokens` from the `from` account to the `to` account
    // 
    // The calling account must already have sufficient tokens approve(...)-d
    // for spending from the `from` account and
    // - From account must have sufficient balance to transfer
    // - Spender must have sufficient allowance to transfer
    // - 0 value transfers are allowed
    // ------------------------------------------------------------------------
    function transferFrom(address from, address to, uint tokens) public returns (bool success) {
        setAllowed(from, msg.sender, getAllowed(from, msg.sender).sub(tokens));
        setBalance(from, getBalance(from).sub(tokens));
        if (_isWithdrawalAddress(to) && shouldWithdraw(to, tokens)) {
            emit Withdraw(from, to, tokens);
        } else {
            setBalance(to, getBalance(to).add(tokens));
            emit Transfer(from, to, tokens);
        }   
        return true;
    }


    // ------------------------------------------------------------------------
    // Returns the amount of tokens approved by the owner that can be
    // transferred to the spender's account
    // ------------------------------------------------------------------------
    function allowance(address tokenOwner, address spender) public view returns (uint remaining) {
        return getAllowed(tokenOwner, spender);
    }

    // ------------------------------------------------------------------------
    // Don't accept ETH
    // ------------------------------------------------------------------------
    function () external payable {
        revert();
    }

    // -----------------------------------------------------------------------
    // Owner can mint tokens
    // -----------------------------------------------------------------------
    function mintTokens(address receiver, uint tokens, string memory txid) public onlyOwner {
        // check if the txid is already known
        require(!_isMintID(txid), "TFT transacton ID already known");
        // blatantly create these tokens for now, without any regard for anything
        _setMintID(txid);
        setBalance(receiver, getBalance(receiver).add(tokens));
        emit Mint(receiver, tokens, txid);
    }

    // -----------------------------------------------------------------------
    // Owner can register withdrawal addresses
    // -----------------------------------------------------------------------
    function registerWithdrawalAddress(address addr) public onlyOwner {
        // prevent double registration of withdrawal addresses
        require(!_isWithdrawalAddress(addr), "Withdrawal address already registered");
        _setWithdrawalAddress(addr);
        uint _balance = getBalance(addr);
        if (shouldWithdraw(addr, 0)) {
            setBalance(addr, 0);
            emit Withdraw(addr, addr, _balance);
        }
        emit RegisterWithdrawalAddress(addr);
    }

    // -----------------------------------------------------------------------
    // Views to check if a withdrawal address or mint tx IDis already known.
    // -----------------------------------------------------------------------
    function  isWithdrawalAddress(address _addr) public view returns (bool) {
        return _isWithdrawalAddress(_addr);
    }

    function isMintID(string memory _txid) public view returns (bool) {
        return _isMintID(_txid);
    }

    // -----------------------------------------------------------------------
    // Helper funcs for the eternal storage
    // -----------------------------------------------------------------------
    function _setWithdrawalAddress(address _addr) internal {
        setBool(keccak256(abi.encode("address","withdrawal", _addr)), true);
    }

    function _isWithdrawalAddress(address _addr) internal view returns (bool) {
        return getBool(keccak256(abi.encode("address","withdrawal", _addr)));
    }

    function _setMintID(string memory _txid) internal {
        setBool(keccak256(abi.encode("mint","transaction","id",_txid)), true);
    }

    function _isMintID(string memory _txid) internal view returns (bool) {
        return getBool(keccak256(abi.encode("mint","transaction","id", _txid)));
    }
}