package(default_visibility = ["//platform/consensus/ordering/td_hotstuff:__subpackages__"])

cc_library(
    name = "proposal_manager",
    srcs = ["proposal_manager.cpp"],
    hdrs = ["proposal_manager.h"],
    deps = [
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
    name = "vote_score_reputation_plugin",
    srcs = ["vote_score_reputation_plugin.cpp"],
    hdrs = ["vote_score_reputation_plugin.h"],
    deps = [
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
        ":proposal_manager",
        ":qc_evidence_recorder",
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
        ":proposal_manager",
        "//common/crypto:mock_signature_verifier",
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
        ":vote_score_reputation_plugin",
        "//common/test:test_main",
    ],
)

cc_test(
    name = "weight_update_manager_test",
    srcs = ["weight_update_manager_test.cpp"],
    deps = [
        ":proposal_manager",
        ":vote_score_reputation_plugin",
        ":weight_update_manager",
        "//common/crypto:mock_signature_verifier",
        "//common/test:test_main",
    ],
)
