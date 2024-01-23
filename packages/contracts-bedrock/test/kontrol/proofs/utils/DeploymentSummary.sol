// SPDX-License-Identifier: MIT
// This file was autogenerated by running `kontrol summary`. Do not edit this file manually.

pragma solidity ^0.8.13;

import { Vm } from "forge-std/Vm.sol";

import { DeploymentSummaryCode } from "./DeploymentSummaryCode.sol";

contract DeploymentSummary is DeploymentSummaryCode {
    // Cheat code address, 0x7109709ECfa91a80626fF3989D68f67F5b1DD12D
    address private constant VM_ADDRESS = address(uint160(uint256(keccak256("hevm cheat code"))));
    Vm private constant vm = Vm(VM_ADDRESS);

    address internal constant AddressManagerAddress = 0xBb2180ebd78ce97360503434eD37fcf4a1Df61c3;
    address internal constant L1CrossDomainMessengerAddress = 0x71fA82Ea96672797954C28032b337aA40AAFC99f;
    address internal constant L1CrossDomainMessengerProxyAddress = 0x0c8b5822b6e02CDa722174F19A1439A7495a3fA6;
    address internal constant L2OutputOracleAddress = 0x19652082F846171168Daf378C4fD3ee85a0D4A60;
    address internal constant L2OutputOracleProxyAddress = 0x8B71b41D4dBEb2b6821d44692d3fACAAf77480Bb;
    address internal constant OptimismPortalAddress = 0x8887E7568E81405c4E0D4cAaabdda949e3B9d4E4;
    address internal constant OptimismPortalProxyAddress = 0x978e3286EB805934215a88694d80b09aDed68D90;
    address internal constant ProtocolVersionsAddress = 0xfbfD64a6C0257F613feFCe050Aa30ecC3E3d7C3F;
    address internal constant ProtocolVersionsProxyAddress = 0x416C42991d05b31E9A6dC209e91AD22b79D87Ae6;
    address internal constant ProxyAdminAddress = 0xDB8cFf278adCCF9E9b5da745B44E754fC4EE3C76;
    address internal constant SafeProxyFactoryAddress = 0x34A1D3fff3958843C43aD80F30b94c510645C316;
    address internal constant SafeSingletonAddress = 0x90193C961A926261B756D1E5bb255e67ff9498A1;
    address internal constant SuperchainConfigAddress = 0x068E44eB31e111028c41598E4535be7468674D0A;
    address internal constant SuperchainConfigProxyAddress = 0xDEb1E9a6Be7Baf84208BB6E10aC9F9bbE1D70809;
    address internal constant SystemConfigAddress = 0xffbA8944650e26653823658d76A122946F27e2f2;
    address internal constant SystemConfigProxyAddress = 0x1c23A6d89F95ef3148BCDA8E242cAb145bf9c0E4;
    address internal constant SystemOwnerSafeAddress = 0x2601573C28B77dea6C8B73385c25024A28a00C3F;

    function recreateDeployment() public {
        bytes32 slot;
        bytes32 value;
        vm.etch(SafeProxyFactoryAddress, SafeProxyFactoryCode);
        vm.etch(SafeSingletonAddress, SafeSingletonCode);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000004";
        value = hex"0000000000000000000000000000000000000000000000000000000000000001";
        vm.store(SafeSingletonAddress, slot, value);
        vm.etch(SystemOwnerSafeAddress, SystemOwnerSafeCode);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000000";
        value = hex"00000000000000000000000090193c961a926261b756d1e5bb255e67ff9498a1";
        vm.store(SystemOwnerSafeAddress, slot, value);
        slot = hex"e90b7bceb6e7df5418fb78d8ee546e97c83a08bbccc01a0644d599ccd2a7c2e0";
        value = hex"0000000000000000000000001804c8ab1f12e6bbf3894d4083f33e07309d1f38";
        vm.store(SystemOwnerSafeAddress, slot, value);
        slot = hex"d1b0d319c6526317dce66989b393dcfb4435c9a65e399a088b63bbf65d7aee32";
        value = hex"0000000000000000000000000000000000000000000000000000000000000001";
        vm.store(SystemOwnerSafeAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000003";
        value = hex"0000000000000000000000000000000000000000000000000000000000000001";
        vm.store(SystemOwnerSafeAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000004";
        value = hex"0000000000000000000000000000000000000000000000000000000000000001";
        vm.store(SystemOwnerSafeAddress, slot, value);
        slot = hex"cc69885fda6bcc1a4ace058b4a62bf5e179ea78fd58a1ccd71c22cc9b688792f";
        value = hex"0000000000000000000000000000000000000000000000000000000000000001";
        vm.store(SystemOwnerSafeAddress, slot, value);
        vm.etch(AddressManagerAddress, AddressManagerCode);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000000";
        value = hex"0000000000000000000000001804c8ab1f12e6bbf3894d4083f33e07309d1f38";
        vm.store(AddressManagerAddress, slot, value);
        vm.etch(ProxyAdminAddress, ProxyAdminCode);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000000";
        value = hex"0000000000000000000000001804c8ab1f12e6bbf3894d4083f33e07309d1f38";
        vm.store(ProxyAdminAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000003";
        value = hex"000000000000000000000000bb2180ebd78ce97360503434ed37fcf4a1df61c3";
        vm.store(ProxyAdminAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000000";
        value = hex"0000000000000000000000002601573c28b77dea6c8b73385c25024a28a00c3f";
        vm.store(ProxyAdminAddress, slot, value);
        vm.etch(SuperchainConfigProxyAddress, SuperchainConfigProxyCode);
        slot = hex"b53127684a568b3173ae13b9f8a6016e243e63b6e8ee1178d6a717850b5d6103";
        value = hex"000000000000000000000000db8cff278adccf9e9b5da745b44e754fc4ee3c76";
        vm.store(SuperchainConfigProxyAddress, slot, value);
        vm.etch(SuperchainConfigAddress, SuperchainConfigCode);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000000";
        value = hex"0000000000000000000000000000000000000000000000000000000000000001";
        vm.store(SuperchainConfigAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000000";
        value = hex"0000000000000000000000000000000000000000000000000000000000000101";
        vm.store(SuperchainConfigAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000000";
        value = hex"0000000000000000000000000000000000000000000000000000000000000001";
        vm.store(SuperchainConfigAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000005";
        value = hex"0000000000000000000000000000000000000000000000000000000000000001";
        vm.store(SystemOwnerSafeAddress, slot, value);
        slot = hex"360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc";
        value = hex"000000000000000000000000068e44eb31e111028c41598e4535be7468674d0a";
        vm.store(SuperchainConfigProxyAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000000";
        value = hex"0000000000000000000000000000000000000000000000000000000000000001";
        vm.store(SuperchainConfigProxyAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000000";
        value = hex"0000000000000000000000000000000000000000000000000000000000000101";
        vm.store(SuperchainConfigProxyAddress, slot, value);
        slot = hex"d30e835d3f35624761057ff5b27d558f97bd5be034621e62240e5c0b784abe68";
        value = hex"0000000000000000000000009965507d1a55bcc2695c58ba16fb37d819b0a4dc";
        vm.store(SuperchainConfigProxyAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000000";
        value = hex"0000000000000000000000000000000000000000000000000000000000000001";
        vm.store(SuperchainConfigProxyAddress, slot, value);
        vm.etch(ProtocolVersionsProxyAddress, ProtocolVersionsProxyCode);
        slot = hex"b53127684a568b3173ae13b9f8a6016e243e63b6e8ee1178d6a717850b5d6103";
        value = hex"000000000000000000000000db8cff278adccf9e9b5da745b44e754fc4ee3c76";
        vm.store(ProtocolVersionsProxyAddress, slot, value);
        vm.etch(ProtocolVersionsAddress, ProtocolVersionsCode);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000000";
        value = hex"0000000000000000000000000000000000000000000000000000000000000001";
        vm.store(ProtocolVersionsAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000000";
        value = hex"0000000000000000000000000000000000000000000000000000000000000101";
        vm.store(ProtocolVersionsAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000033";
        value = hex"0000000000000000000000004e59b44847b379578588920ca78fbf26c0b4956c";
        vm.store(ProtocolVersionsAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000033";
        value = hex"000000000000000000000000000000000000000000000000000000000000dead";
        vm.store(ProtocolVersionsAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000000";
        value = hex"0000000000000000000000000000000000000000000000000000000000000001";
        vm.store(ProtocolVersionsAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000005";
        value = hex"0000000000000000000000000000000000000000000000000000000000000002";
        vm.store(SystemOwnerSafeAddress, slot, value);
        slot = hex"360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc";
        value = hex"000000000000000000000000fbfd64a6c0257f613fefce050aa30ecc3e3d7c3f";
        vm.store(ProtocolVersionsProxyAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000000";
        value = hex"0000000000000000000000000000000000000000000000000000000000000001";
        vm.store(ProtocolVersionsProxyAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000000";
        value = hex"0000000000000000000000000000000000000000000000000000000000000101";
        vm.store(ProtocolVersionsProxyAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000033";
        value = hex"000000000000000000000000db8cff278adccf9e9b5da745b44e754fc4ee3c76";
        vm.store(ProtocolVersionsProxyAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000033";
        value = hex"0000000000000000000000009965507d1a55bcc2695c58ba16fb37d819b0a4dc";
        vm.store(ProtocolVersionsProxyAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000000";
        value = hex"0000000000000000000000000000000000000000000000000000000000000001";
        vm.store(ProtocolVersionsProxyAddress, slot, value);
        vm.etch(OptimismPortalProxyAddress, OptimismPortalProxyCode);
        slot = hex"b53127684a568b3173ae13b9f8a6016e243e63b6e8ee1178d6a717850b5d6103";
        value = hex"000000000000000000000000db8cff278adccf9e9b5da745b44e754fc4ee3c76";
        vm.store(OptimismPortalProxyAddress, slot, value);
        vm.etch(L2OutputOracleProxyAddress, L2OutputOracleProxyCode);
        slot = hex"b53127684a568b3173ae13b9f8a6016e243e63b6e8ee1178d6a717850b5d6103";
        value = hex"000000000000000000000000db8cff278adccf9e9b5da745b44e754fc4ee3c76";
        vm.store(L2OutputOracleProxyAddress, slot, value);
        vm.etch(SystemConfigProxyAddress, SystemConfigProxyCode);
        slot = hex"b53127684a568b3173ae13b9f8a6016e243e63b6e8ee1178d6a717850b5d6103";
        value = hex"000000000000000000000000db8cff278adccf9e9b5da745b44e754fc4ee3c76";
        vm.store(SystemConfigProxyAddress, slot, value);
        vm.etch(L1CrossDomainMessengerProxyAddress, L1CrossDomainMessengerProxyCode);
        slot = hex"a8f0d50211ac8ff1a40793a899dff3ced4762e0466f69b0078ab7b00d037835c";
        value = hex"000000000000000000000000bb2180ebd78ce97360503434ed37fcf4a1df61c3";
        vm.store(L1CrossDomainMessengerProxyAddress, slot, value);
        slot = hex"eae32376463217751b8fa4dea8c38ab253664fa3605de6d85d2e790aa970f2b8";
        value = hex"4f564d5f4c3143726f7373446f6d61696e4d657373656e676572000000000034";
        vm.store(L1CrossDomainMessengerProxyAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000000";
        value = hex"000000000000000000000000db8cff278adccf9e9b5da745b44e754fc4ee3c76";
        vm.store(AddressManagerAddress, slot, value);
        vm.etch(OptimismPortalAddress, OptimismPortalCode);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000000";
        value = hex"0000000000000000000000000000000000000000000000000000000000000001";
        vm.store(OptimismPortalAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000000";
        value = hex"0000000000000000000000000000000000000000000000000000000000000101";
        vm.store(OptimismPortalAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000032";
        value = hex"000000000000000000000000000000000000000000000000000000000000dead";
        vm.store(OptimismPortalAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000001";
        value = hex"000000000000000100000000000000000000000000000000000000003b9aca00";
        vm.store(OptimismPortalAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000000";
        value = hex"0000000000000000000000000000000000000000000000000000000000000001";
        vm.store(OptimismPortalAddress, slot, value);
        vm.etch(L1CrossDomainMessengerAddress, L1CrossDomainMessengerCode);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000000";
        value = hex"0000000000000000000000010000000000000000000000000000000000000000";
        vm.store(L1CrossDomainMessengerAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000000";
        value = hex"0000000000000000000001010000000000000000000000000000000000000000";
        vm.store(L1CrossDomainMessengerAddress, slot, value);
        slot = hex"00000000000000000000000000000000000000000000000000000000000000cc";
        value = hex"000000000000000000000000000000000000000000000000000000000000dead";
        vm.store(L1CrossDomainMessengerAddress, slot, value);
        slot = hex"00000000000000000000000000000000000000000000000000000000000000cf";
        value = hex"0000000000000000000000004200000000000000000000000000000000000007";
        vm.store(L1CrossDomainMessengerAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000000";
        value = hex"0000000000000000000000010000000000000000000000000000000000000000";
        vm.store(L1CrossDomainMessengerAddress, slot, value);
        vm.etch(L2OutputOracleAddress, L2OutputOracleCode);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000000";
        value = hex"0000000000000000000000000000000000000000000000000000000000000001";
        vm.store(L2OutputOracleAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000000";
        value = hex"0000000000000000000000000000000000000000000000000000000000000101";
        vm.store(L2OutputOracleAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000004";
        value = hex"0000000000000000000000000000000000000000000000000000000000000001";
        vm.store(L2OutputOracleAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000005";
        value = hex"0000000000000000000000000000000000000000000000000000000000000001";
        vm.store(L2OutputOracleAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000000";
        value = hex"0000000000000000000000000000000000000000000000000000000000000001";
        vm.store(L2OutputOracleAddress, slot, value);
        vm.etch(SystemConfigAddress, SystemConfigCode);
        slot = hex"a11ee3ab75b40e88a0105e935d17cd36c8faee0138320d776c411291bdbbb19f";
        value = hex"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
        vm.store(SystemConfigAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000000";
        value = hex"0000000000000000000000000000000000000000000000000000000000000001";
        vm.store(SystemConfigAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000000";
        value = hex"0000000000000000000000000000000000000000000000000000000000000101";
        vm.store(SystemConfigAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000033";
        value = hex"0000000000000000000000004e59b44847b379578588920ca78fbf26c0b4956c";
        vm.store(SystemConfigAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000033";
        value = hex"000000000000000000000000000000000000000000000000000000000000dead";
        vm.store(SystemConfigAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000068";
        value = hex"0000000000000000000000000000000000000000000000000000000000000001";
        vm.store(SystemConfigAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000069";
        value = hex"0000000000000000000000000000000000000000000000000000020100000001";
        vm.store(SystemConfigAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000000";
        value = hex"0000000000000000000000000000000000000000000000000000000000000001";
        vm.store(SystemConfigAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000005";
        value = hex"0000000000000000000000000000000000000000000000000000000000000003";
        vm.store(SystemOwnerSafeAddress, slot, value);
        slot = hex"360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc";
        value = hex"000000000000000000000000ffba8944650e26653823658d76a122946f27e2f2";
        vm.store(SystemConfigProxyAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000000";
        value = hex"0000000000000000000000000000000000000000000000000000000000000001";
        vm.store(SystemConfigProxyAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000000";
        value = hex"0000000000000000000000000000000000000000000000000000000000000101";
        vm.store(SystemConfigProxyAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000033";
        value = hex"000000000000000000000000db8cff278adccf9e9b5da745b44e754fc4ee3c76";
        vm.store(SystemConfigProxyAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000033";
        value = hex"0000000000000000000000009965507d1a55bcc2695c58ba16fb37d819b0a4dc";
        vm.store(SystemConfigProxyAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000067";
        value = hex"0000000000000000000000003c44cdddb6a900fa2b585dd299e03d12fa4293bc";
        vm.store(SystemConfigProxyAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000065";
        value = hex"0000000000000000000000000000000000000000000000000000000000000834";
        vm.store(SystemConfigProxyAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000066";
        value = hex"00000000000000000000000000000000000000000000000000000000000f4240";
        vm.store(SystemConfigProxyAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000068";
        value = hex"00000000000000000000000000000000000000000000000000000000017d7840";
        vm.store(SystemConfigProxyAddress, slot, value);
        slot = hex"65a7ed542fb37fe237fdfbdd70b31598523fe5b32879e307bae27a0bd9581c08";
        value = hex"0000000000000000000000009965507d1a55bcc2695c58ba16fb37d819b0a4dc";
        vm.store(SystemConfigProxyAddress, slot, value);
        slot = hex"71ac12829d66ee73d8d95bff50b3589745ce57edae70a3fb111a2342464dc597";
        value = hex"000000000000000000000000ff00000000000000000000000000000000000000";
        vm.store(SystemConfigProxyAddress, slot, value);
        slot = hex"383f291819e6d54073bc9a648251d97421076bdd101933c0c022219ce9580636";
        value = hex"0000000000000000000000000c8b5822b6e02cda722174f19a1439a7495a3fa6";
        vm.store(SystemConfigProxyAddress, slot, value);
        slot = hex"e52a667f71ec761b9b381c7b76ca9b852adf7e8905da0e0ad49986a0a6871815";
        value = hex"0000000000000000000000008b71b41d4dbeb2b6821d44692d3facaaf77480bb";
        vm.store(SystemConfigProxyAddress, slot, value);
        slot = hex"4b6c74f9e688cb39801f2112c14a8c57232a3fc5202e1444126d4bce86eb19ac";
        value = hex"000000000000000000000000978e3286eb805934215a88694d80b09aded68d90";
        vm.store(SystemConfigProxyAddress, slot, value);
        slot = hex"a11ee3ab75b40e88a0105e935d17cd36c8faee0138320d776c411291bdbbb19f";
        value = hex"0000000000000000000000000000000000000000000000000000000000000001";
        vm.store(SystemConfigProxyAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000069";
        value = hex"0000ffffffffffffffffffffffffffffffff000f42403b9aca00080a01312d00";
        vm.store(SystemConfigProxyAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000000";
        value = hex"0000000000000000000000000000000000000000000000000000000000000001";
        vm.store(SystemConfigProxyAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000005";
        value = hex"0000000000000000000000000000000000000000000000000000000000000004";
        vm.store(SystemOwnerSafeAddress, slot, value);
        slot = hex"a8f0d50211ac8ff1a40793a899dff3ced4762e0466f69b0078ab7b00d037835c";
        value = hex"0000000000000000000000000000000000000000000000000000000000000002";
        vm.store(ProxyAdminAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000005";
        value = hex"0000000000000000000000000000000000000000000000000000000000000005";
        vm.store(SystemOwnerSafeAddress, slot, value);
        slot = hex"7cda2d9a7dd1a58982b7fac9315bdc1ed8c92aeb9c22cd8555aaa54972f01ccb";
        value = hex"4f564d5f4c3143726f7373446f6d61696e4d657373656e676572000000000034";
        vm.store(ProxyAdminAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000005";
        value = hex"0000000000000000000000000000000000000000000000000000000000000006";
        vm.store(SystemOwnerSafeAddress, slot, value);
        slot = hex"515216935740e67dfdda5cf8e248ea32b3277787818ab59153061ac875c9385e";
        value = hex"00000000000000000000000071fa82ea96672797954c28032b337aa40aafc99f";
        vm.store(AddressManagerAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000000";
        value = hex"0000000000000000000000010000000000000000000000000000000000000000";
        vm.store(L1CrossDomainMessengerProxyAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000000";
        value = hex"0000000000000000000001010000000000000000000000000000000000000000";
        vm.store(L1CrossDomainMessengerProxyAddress, slot, value);
        slot = hex"00000000000000000000000000000000000000000000000000000000000000fb";
        value = hex"000000000000000000000000deb1e9a6be7baf84208bb6e10ac9f9bbe1d70809";
        vm.store(L1CrossDomainMessengerProxyAddress, slot, value);
        slot = hex"00000000000000000000000000000000000000000000000000000000000000fc";
        value = hex"000000000000000000000000978e3286eb805934215a88694d80b09aded68d90";
        vm.store(L1CrossDomainMessengerProxyAddress, slot, value);
        slot = hex"00000000000000000000000000000000000000000000000000000000000000cc";
        value = hex"000000000000000000000000000000000000000000000000000000000000dead";
        vm.store(L1CrossDomainMessengerProxyAddress, slot, value);
        slot = hex"00000000000000000000000000000000000000000000000000000000000000cf";
        value = hex"0000000000000000000000004200000000000000000000000000000000000007";
        vm.store(L1CrossDomainMessengerProxyAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000000";
        value = hex"0000000000000000000000010000000000000000000000000000000000000000";
        vm.store(L1CrossDomainMessengerProxyAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000005";
        value = hex"0000000000000000000000000000000000000000000000000000000000000007";
        vm.store(SystemOwnerSafeAddress, slot, value);
        slot = hex"360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc";
        value = hex"0000000000000000000000008887e7568e81405c4e0d4caaabdda949e3b9d4e4";
        vm.store(OptimismPortalProxyAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000000";
        value = hex"0000000000000000000000000000000000000000000000000000000000000001";
        vm.store(OptimismPortalProxyAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000000";
        value = hex"0000000000000000000000000000000000000000000000000000000000000101";
        vm.store(OptimismPortalProxyAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000036";
        value = hex"0000000000000000000000008b71b41d4dbeb2b6821d44692d3facaaf77480bb";
        vm.store(OptimismPortalProxyAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000037";
        value = hex"0000000000000000000000001c23a6d89f95ef3148bcda8e242cab145bf9c0e4";
        vm.store(OptimismPortalProxyAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000035";
        value = hex"0000000000000000000000deb1e9a6be7baf84208bb6e10ac9f9bbe1d7080900";
        vm.store(OptimismPortalProxyAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000032";
        value = hex"000000000000000000000000000000000000000000000000000000000000dead";
        vm.store(OptimismPortalProxyAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000001";
        value = hex"000000000000000100000000000000000000000000000000000000003b9aca00";
        vm.store(OptimismPortalProxyAddress, slot, value);
        slot = hex"0000000000000000000000000000000000000000000000000000000000000000";
        value = hex"0000000000000000000000000000000000000000000000000000000000000001";
        vm.store(OptimismPortalProxyAddress, slot, value);
    }
}
