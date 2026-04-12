package(default_visibility = ["//platform/consensus/ordering/td_hotstuff:__subpackages__"])

cc_library(
    name = "proposal_manager",
    srcs = ["proposal_manager.cpp"],
    hdrs = ["proposal_manager.h"],
    deps = [
        "//common:comm",
        "//common/crypto:hash",
        "//common/crypto:signature_verifier",
        "//common/utils",
        "//platform/consensus/ordering/td_hotstuff/proto:proposal_cc_proto",
        "//platform/statistic:stats",
    ],
)

cc_library(
    name = "td_hotstuff",
    srcs = ["td_hotstuff.cpp"],
    hdrs = ["td_hotstuff.h"],
    deps = [
        ":proposal_manager",
        "//common:comm",
        "//common/crypto:hash",
        "//platform/common/queue:lock_free_queue",
        "//platform/consensus/ordering/common/algorithm:protocol_base",
        "//platform/statistic:stats",
    ],
)

