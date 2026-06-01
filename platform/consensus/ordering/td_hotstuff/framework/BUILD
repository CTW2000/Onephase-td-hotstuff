package(default_visibility = ["//visibility:private"])

cc_library(
    name = "consensus",
    srcs = ["consensus.cpp"],
    hdrs = ["consensus.h"],
    visibility = [
        "//visibility:public",
    ],
    deps = [
        ":performance_manager",
        "//common/utils",
        "//executor/common:transaction_manager",
        "//platform/consensus/ordering/common/framework:consensus",
        "//platform/consensus/execution:transaction_executor",
        "//platform/consensus/ordering/td_hotstuff/algorithm:td_hotstuff",
        "//platform/networkstrate:consensus_manager",
    ],
)

cc_library(
    name = "performance_manager",
    srcs = ["performance_manager.cpp"],
    hdrs = ["performance_manager.h"],
    deps = [
        "//platform/consensus/ordering/common/framework:performance_manager",
        "//platform/consensus/ordering/td_hotstuff/algorithm:leader_selection_schedule",
    ],
)

cc_test(
    name = "performance_manager_test",
    srcs = ["performance_manager_test.cpp"],
    deps = [
        ":performance_manager",
        "//common/test:test_main",
    ],
)
