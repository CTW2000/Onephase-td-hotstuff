package(default_visibility = ["//platform/consensus/ordering/td_hotstuff:__subpackages__"])

cc_library(
    name = "proposal_manager",
    srcs = ["proposal_manager.cpp"],
    hdrs = ["proposal_manager.h"],
    deps = [
        ":certificate_verifier",
        ":safety_rules",
        ":leader_selection_schedule",
        ":timeout_manager",
        ":weight_schedule",
        "//common:comm",
        "//common/crypto:signature_verifier",
        "//common/utils",
        "//platform/consensus/ordering/td_hotstuff/proto:proposal_cc_proto",
        "//platform/statistic:stats",
    ],
)

cc_library(
    name = "async_consensus_verifier",
    srcs = ["async_consensus_verifier.cpp"],
    hdrs = ["async_consensus_verifier.h"],
    deps = [
        ":certificate_verifier",
        "//common:glog",
        "//platform/consensus/ordering/td_hotstuff/proto:proposal_cc_proto",
    ],
)

cc_library(
    name = "certificate_verifier",
    srcs = ["certificate_verifier.cpp"],
    hdrs = ["certificate_verifier.h"],
    deps = [
        ":weight_schedule",
        "//common:glog",
        "//common/crypto:signature_verifier",
        "//platform/consensus/ordering/td_hotstuff/proto:proposal_cc_proto",
    ],
)

cc_library(
    name = "safety_rules",
    srcs = ["safety_rules.cpp"],
    hdrs = ["safety_rules.h"],
    deps = ["//platform/consensus/ordering/td_hotstuff/proto:proposal_cc_proto"],
)

cc_library(
    name = "timeout_manager",
    srcs = ["timeout_manager.cpp"],
    hdrs = ["timeout_manager.h"],
    deps = [
        ":certificate_verifier",
        ":weight_schedule",
        "//common:glog",
        "//common/crypto:signature_verifier",
        "//platform/consensus/ordering/td_hotstuff/proto:proposal_cc_proto",
    ],
)

cc_library(
    name = "td_hotstuff_fault_injector",
    srcs = ["td_hotstuff_fault_injector.cpp"],
    hdrs = ["td_hotstuff_fault_injector.h"],
    deps = [
        ":certificate_verifier",
        ":weight_update_manager",
        "//common:glog",
        "//common/crypto:signature_verifier",
        "//platform/consensus/ordering/td_hotstuff/proto:proposal_cc_proto",
    ],
)

cc_library(
    name = "td_hotstuff_digest",
    srcs = ["td_hotstuff_digest.cpp"],
    hdrs = ["td_hotstuff_digest.h"],
    deps = ["//common/crypto:hash"],
)

cc_library(
    name = "weight_schedule",
    srcs = ["weight_schedule.cpp"],
    hdrs = ["weight_schedule.h"],
    deps = [":td_hotstuff_digest"],
)

cc_library(
    name = "leader_selection_schedule",
    srcs = ["leader_selection_schedule.cpp"],
    hdrs = ["leader_selection_schedule.h"],
    deps = [
        ":td_hotstuff_digest",
        ":weight_schedule",
    ],
)

cc_library(
    name = "qc_signer_selector",
    srcs = ["qc_signer_selector.cpp"],
    hdrs = ["qc_signer_selector.h"],
)

cc_library(
    name = "vote_score_reputation_plugin",
    srcs = ["vote_score_reputation_plugin.cpp"],
    hdrs = ["vote_score_reputation_plugin.h"],
    deps = [
        ":td_hotstuff_digest",
        "//common:glog",
        "//common/crypto:hash",
        "//platform/consensus/reputation:reputation_algorithm",
    ],
)


cc_library(
    name = "qc_evidence_recorder",
    srcs = ["qc_evidence_recorder.cpp"],
    hdrs = ["qc_evidence_recorder.h"],
    deps = [
        ":vote_score_reputation_plugin",
        "//common:glog",
        "//platform/consensus/reputation:reputation_algorithm",
    ],
)

cc_library(
    name = "weight_update_manager",
    srcs = ["weight_update_manager.cpp"],
    hdrs = ["weight_update_manager.h"],
    deps = [
        ":leader_selection_schedule",
        ":proposal_manager",
        ":vote_score_reputation_plugin",
        ":weight_schedule",
        "//common:glog",
        "//common/crypto:hash",
        "//common/crypto:signature_verifier",
        "//platform/consensus/ordering/td_hotstuff/proto:proposal_cc_proto",
    ],
)

cc_library(
    name = "td_hotstuff",
    srcs = ["td_hotstuff.cpp"],
    hdrs = ["td_hotstuff.h"],
    deps = [
        ":async_consensus_verifier",
        ":leader_selection_schedule",
        ":proposal_manager",
        ":qc_evidence_recorder",
        ":qc_signer_selector",
        ":td_hotstuff_fault_injector",
        ":timeout_manager",
        ":weight_update_manager",
        "//common:comm",
        "//platform/common/queue:lock_free_queue",
        "//platform/consensus/ordering/common/algorithm:protocol_base",
        "//platform/statistic:stats",
    ],
)



cc_test(
    name = "certificate_verifier_test",
    srcs = ["certificate_verifier_test.cpp"],
    deps = [
        ":certificate_verifier",
        ":proposal_manager",
        "//common/crypto:mock_signature_verifier",
        "//common/test:test_main",
    ],
)

cc_test(
    name = "async_consensus_verifier_test",
    srcs = ["async_consensus_verifier_test.cpp"],
    deps = [
        ":async_consensus_verifier",
        ":proposal_manager",
        "//common/crypto:mock_signature_verifier",
        "//common/test:test_main",
    ],
)

cc_test(
    name = "safety_rules_test",
    srcs = ["safety_rules_test.cpp"],
    deps = [
        ":proposal_manager",
        ":safety_rules",
        "//common/test:test_main",
    ],
)

cc_test(
    name = "td_hotstuff_weight_threshold_check",
    srcs = ["td_hotstuff_weight_threshold_check.cpp"],
    deps = [
        ":leader_selection_schedule",
        ":proposal_manager",
        ":td_hotstuff_fault_injector",
        "//common/crypto:mock_signature_verifier",
        "//common/test:test_main",
    ],
)

cc_test(
    name = "timeout_manager_test",
    srcs = ["timeout_manager_test.cpp"],
    deps = [
        ":timeout_manager",
        ":proposal_manager",
        "//common/crypto:mock_signature_verifier",
        "//common/test:test_main",
    ],
)

cc_test(
    name = "td_hotstuff_fault_injector_test",
    srcs = ["td_hotstuff_fault_injector_test.cpp"],
    deps = [
        ":proposal_manager",
        ":td_hotstuff_fault_injector",
        ":timeout_manager",
        ":weight_update_manager",
        "//common/crypto:mock_signature_verifier",
        "//common/test:test_main",
    ],
)

cc_test(
    name = "leader_selection_schedule_test",
    srcs = ["leader_selection_schedule_test.cpp"],
    deps = [
        ":leader_selection_schedule",
        "//common/test:test_main",
    ],
)

cc_test(
    name = "qc_signer_selector_test",
    srcs = ["qc_signer_selector_test.cpp"],
    deps = [
        ":qc_signer_selector",
        "//common/test:test_main",
    ],
)


cc_test(
    name = "qc_evidence_recorder_test",
    srcs = ["qc_evidence_recorder_test.cpp"],
    deps = [
        ":qc_evidence_recorder",
        "//common/test:test_main",
    ],
)

cc_test(
    name = "vote_score_reputation_plugin_test",
    srcs = ["vote_score_reputation_plugin_test.cpp"],
    deps = [
        ":leader_selection_schedule",
        ":vote_score_reputation_plugin",
        "//common/test:test_main",
    ],
)

cc_test(
    name = "weight_update_manager_test",
    srcs = ["weight_update_manager_test.cpp"],
    deps = [
        ":leader_selection_schedule",
        ":proposal_manager",
        ":vote_score_reputation_plugin",
        ":weight_update_manager",
        "//common/crypto:mock_signature_verifier",
        "//common/test:test_main",
    ],
)
