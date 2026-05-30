package(default_visibility = ["//platform/consensus/ordering/td_hotstuff:__subpackages__"])

cc_library(
    name = "proposal_manager",
    srcs = ["proposal_manager.cpp"],
    hdrs = ["proposal_manager.h"],
    deps = [
        "//common:comm",
        "//common/crypto:signature_verifier",
        "//common/utils",
        "//platform/consensus/ordering/td_hotstuff/proto:proposal_cc_proto",
        "//platform/statistic:stats",
    ],
)


cc_library(
    name = "qc_evidence_recorder",
    srcs = ["qc_evidence_recorder.cpp"],
    hdrs = ["qc_evidence_recorder.h"],
    deps = [
        "//common:glog",
    ],
)

cc_library(
    name = "td_hotstuff",
    srcs = ["td_hotstuff.cpp"],
    hdrs = ["td_hotstuff.h"],
    deps = [
        ":proposal_manager",
        ":qc_evidence_recorder",
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
