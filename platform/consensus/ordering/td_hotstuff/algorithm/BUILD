package(default_visibility = ["//platform/consensus/ordering/td_hotstuff:__subpackages__"])

cc_library(
    name = "proposal_manager",
    srcs = ["proposal_manager.cpp"],
    hdrs = ["proposal_manager.h"],
    deps = [
        ":leader_selection_schedule",
        ":weight_schedule",
        "//common:comm",
        "//common/crypto:signature_verifier",
        "//common/utils",
        "//platform/consensus/ordering/td_hotstuff/proto:proposal_cc_proto",
        "//platform/statistic:stats",
    ],
)

cc_library(
    name = "weight_schedule",
    srcs = ["weight_schedule.cpp"],
    hdrs = ["weight_schedule.h"],
    deps = ["//common/crypto:hash"],
)

cc_library(
    name = "leader_selection_schedule",
    srcs = ["leader_selection_schedule.cpp"],
    hdrs = ["leader_selection_schedule.h"],
    deps = [
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
        ":leader_selection_schedule",
        ":weight_schedule",
        "//common:glog",
        "//common/crypto:hash",
    ],
)


cc_library(
    name = "qc_evidence_recorder",
    srcs = ["qc_evidence_recorder.cpp"],
    hdrs = ["qc_evidence_recorder.h"],
    deps = [
        ":vote_score_reputation_plugin",
        "//common:glog",
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
        ":leader_selection_schedule",
        ":proposal_manager",
        ":qc_evidence_recorder",
        ":qc_signer_selector",
        ":weight_update_manager",
        "//common:comm",
        "//platform/common/queue:lock_free_queue",
        "//platform/consensus/ordering/common/algorithm:protocol_base",
        "//platform/statistic:stats",
    ],
)


cc_test(
    name = "td_hotstuff_weight_threshold_check",
    srcs = ["td_hotstuff_weight_threshold_check.cpp"],
    deps = [
        ":leader_selection_schedule",
        ":proposal_manager",
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
