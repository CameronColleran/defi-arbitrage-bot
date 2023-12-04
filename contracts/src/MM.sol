// SPDX-License-Identifier: UNLICENSED

import "joe/interfaces/ILBFactory.sol";
import "joe/interfaces/ILBPair.sol";
import "joe/interfaces/ILBRouter.sol";
import "joe/interfaces/ILBToken.sol";
import "openzeppelin/token/ERC20/IERC20.sol";

pragma solidity ^0.8.10;

contract MM {
    address public owner;
    mapping(address => bool) public whitelistedCallers;

    ILBPair public constant _LBPair = ILBPair(0x7eC3717f70894F6d9BA0be00774610394Ce006eE);
    IERC20 public immutable X;
    IERC20 public immutable Y;


    struct Call {
        address target;
        bytes callData;
    }
    struct Result {
        bool success;
        bytes returnData;
    }

    struct Meta {
        uint256 curid;
        uint256 amountX;
        uint256 amountY;
    }


    constructor() {
        whitelistedCallers[msg.sender] = true;
        owner = msg.sender;
        X = IERC20(_LBPair.tokenX());
        Y = IERC20(_LBPair.tokenY());
        X.approve(msg.sender, type(uint256).max);
        Y.approve(msg.sender, type(uint256).max);
    }

    modifier onlyWhitelist() {
        require(whitelistedCallers[msg.sender]);
        _;
    }
    modifier onlyOwner() {
        require(owner == msg.sender);
        _;
    }

    function myBins(uint256[] calldata _ids) external view returns (uint256[] memory batchBalances) {
        uint256 l = _ids.length;
        batchBalances = new uint256[](l);
        for (uint256 i; i < l; i++) {
            batchBalances[i] = ILBToken(address(_LBPair)).balanceOf(address(this), _ids[i]);
        }
    }

    function supply(uint256[] calldata _ids) external view returns (uint256[] memory batchBalances) {
        uint256 l = _ids.length;
        batchBalances = new uint256[](l);
        for (uint256 i; i < l; i++) {
            batchBalances[i] = ILBToken(address(_LBPair)).totalSupply( _ids[i]);
        }
    }

    function getReserves(uint24[] calldata ids) external view returns (uint256[] memory _reserveX, uint256[] memory _reserveY) {
        uint256 l = ids.length;
        _reserveX = new uint256[](l);
        _reserveY = new uint256[](l);
        for (uint256 i; i < l; i++) {
            (_reserveX[i], _reserveY[i]) = _LBPair.getBin(ids[i]);
        }
    }

    function left(uint24 start, uint256 n) external view returns (uint24[] memory ids) {
        uint24 curId = start;
        ids = new uint24[](n);
        for (uint256 i; i < n; i++) {
            try _LBPair.findFirstNonEmptyBinId(curId, true) returns (uint24 id) {
                ids[i] = id;
                curId = id;
            } catch {
                break;
            }
        }
    }

    function right(uint24 start, uint256 n) external view returns (uint24[] memory ids) {
        uint24 curId = start;
        ids = new uint24[](n);
        for (uint256 i; i < n; i++) {
            try _LBPair.findFirstNonEmptyBinId(curId, false) returns (uint24 id) {
                ids[i] = id;
                curId = id;
            } catch {
                break;
            }
        }
    }



    function make(
        uint256 curid,
        uint256 amountX,
        uint256 amountY,
        uint256[] calldata _ids,
        uint256[] calldata _distributionX,
        uint256[] calldata _distributionY
    ) external onlyWhitelist {
        {
            (, , uint256 activeId) = _LBPair.getReservesAndId();
            require(curid == activeId, "ID");
        }
        

        X.transfer(address(_LBPair), amountX);
        Y.transfer(address(_LBPair), amountY);
        _LBPair.mint(
                _ids,
                _distributionX,
                _distributionY,
                address(this)
            );
    }

    function move(
        Meta calldata _meta,
        uint256[] calldata _idsIn,
        uint256[] calldata _distributionX,
        uint256[] calldata _distributionY,
        uint256[] calldata _idsOut,
        uint256[] calldata _amounts
    ) external onlyWhitelist {
        {
            (, , uint256 activeId) = _LBPair.getReservesAndId();
            require(_meta.curid == activeId, "ID");
        }
        {
            ILBToken(address(_LBPair)).safeBatchTransferFrom(address(this), address(_LBPair), _idsOut, _amounts);
            _LBPair.burn(_idsOut, _amounts, address(this));
        }
        
        {
            X.transfer(address(_LBPair), _meta.amountX);
            Y.transfer(address(_LBPair), _meta.amountY);
            _LBPair.mint(
                    _idsIn,
                    _distributionX,
                    _distributionY,
                    address(this)
                );
        }
        
    }

    function cancel(
        uint256[] calldata _ids,
        uint256[] calldata _amounts
    ) external onlyWhitelist {
        ILBToken(address(_LBPair)).safeBatchTransferFrom(address(this), address(_LBPair), _ids, _amounts);
        _LBPair.burn(_ids, _amounts, address(this));
    }

    function take(
        uint256 curid,
        uint256 amountX,
        uint256 amountY,
        bool swapForY
    ) external onlyWhitelist {
        {
            (, , uint256 activeId) = _LBPair.getReservesAndId();
            require(curid == activeId, "ID");
        }
        if (swapForY) {
            X.transfer(address(_LBPair), amountX);
            (, uint256 _amountYOut) = _LBPair.swap(true, address(this));
            require(_amountYOut >= amountY, "Y_S");
        } else {
            Y.transfer(address(_LBPair), amountY);
            (uint256 _amountXOut,) = _LBPair.swap(false, address(this));
            require(_amountXOut >= amountX, "X_S");
        }
        
    }

    function cancelNTake(
        uint256 curid,
        uint256 amountX,
        uint256 amountY,
        bool swapForY,
        uint256[] calldata _ids,
        uint256[] calldata _amounts
    ) external onlyWhitelist {
        {
            (, , uint256 activeId) = _LBPair.getReservesAndId();
            require(curid == activeId, "ID");
        }
        ILBToken(address(_LBPair)).safeBatchTransferFrom(address(this), address(_LBPair), _ids, _amounts);
        _LBPair.burn(_ids, _amounts, address(this));

        if (swapForY) {
            X.transfer(address(_LBPair), amountX);
            (, uint256 _amountYOut) = _LBPair.swap(true, address(this));
            require(_amountYOut >= amountY, "Y_S");
        } else {
            Y.transfer(address(_LBPair), amountY);
            (uint256 _amountXOut,) = _LBPair.swap(false, address(this));
            require(_amountXOut >= amountX, "X_S");
        }
    }

    function getRewards(uint256[] calldata _ids) external onlyWhitelist {
        _LBPair.collectFees(address(this), _ids);
    }


    function setWhitelist(address _caller, bool _status) external onlyOwner {
        whitelistedCallers[_caller] = _status;
    }

    function sweep(address token, uint amt) external onlyOwner {
        if (token == address(0)) {
            (bool sent, 
            // bytes memory data
            ) = payable(owner).call{
                value: amt
            }("");
            require(sent, "Failed to send Ether");
        } else {
            IERC20(token).transfer(
                owner,
                amt
            );
        }
    }

    function sweep(address token) external onlyOwner {
        if (token == address(0)) {
            (bool sent, 
            // bytes memory data
            ) = payable(owner).call{
                value: address(this).balance
            }("");
            require(sent, "Failed to send Ether");
        } else {
            IERC20(token).transfer(
                owner,
                IERC20(token).balanceOf(address(this))
            );
        }
    }

    function execute(Call[] memory calls)
        external onlyOwner
        returns (bytes[] memory returnData)
    {
        require(msg.sender == owner, "Only owner can execute");
        returnData = new bytes[](calls.length);
        for (uint256 i = 0; i < calls.length; i++) {
            (bool success, bytes memory ret) = calls[i].target.call(
                calls[i].callData
            );
            require(success, "Multicall aggregate: call failed");
            returnData[i] = ret;
        }
    }

    receive() external payable {}

}
