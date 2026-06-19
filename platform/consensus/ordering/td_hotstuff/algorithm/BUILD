package(default_visibility = ["//platform/consensus/ordering/td_hotstuff:__subpackages__"])

cc_library(
    name = "proposal_manager",
    srcs = ["proposal_manager.cpp"],
    hdrs = ["proposal_manager.h"],
    deps = [
        ":certificate_verifier",
        ":leader_selection_schedule",
        ":safety_rules",
        ":timeout_manager",
        ":weight_schedule",
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
        "//common:glog",
    ],
)

cc_library(
    name = "weight_update_controller",
    srcs = ["weight_update_controller.cpp"],
    hdrs = ["weight_update_controller.h"],
    deps = [
        ":leader_selection_schedule",
        ":weight_schedule",
        "//common:glog",
        "//common/crypto:signature_verifier",
        "//platform/consensus/ordering/td_hotstuff/proto:proposal_cc_proto",
        "//platform/consensus/reputation:reputation_algorithm",
    ],
)

cc_library(
    name = "weight_update_experiment",
    srcs = ["weight_update_experiment.cpp"],
    hdrs = ["weight_update_experiment.h"],
    deps = [
        ":weight_update_controller",
        "//common/crypto:signature_verifier",
        "//platform/consensus/ordering/td_hotstuff/proto:proposal_cc_proto",
        "//platform/consensus/reputation:reputation_algorithm",
    ],
)

cc_library(
    name = "certified_weight_update_pipeline",
    srcs = ["certified_weight_update_pipeline.cpp"],
    hdrs = ["certified_weight_update_pipeline.h"],
    deps = [
        ":leader_selection_schedule",
        ":weight_schedule",
        ":weight_update_controller",
        ":weight_update_experiment",
        "//common:glog",
        "//common/crypto:signature_verifier",
        "//platform/consensus/ordering/td_hotstuff/adapter:td_hotstuff_reputation_adapter",
        "//platform/consensus/ordering/td_hotstuff/proto:proposal_cc_proto",
        "//platform/consensus/reputation:reputation_algorithm",
    ],
)

cc_library(
    name = "td_hotstuff",
    srcs = ["td_hotstuff.cpp"],
    hdrs = ["td_hotstuff.h"],
    deps = [
        ":async_consensus_verifier",
        ":certified_weight_update_pipeline",
        ":leader_selection_schedule",
        ":proposal_manager",
        ":timeout_manager",
        ":weight_schedule",
        "//common:comm",
        "//platform/consensus/ordering/td_hotstuff/adapter:td_hotstuff_reputation_adapter",
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
        ":proposal_manager",
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
    name = "leader_selection_schedule_test",
    srcs = ["leader_selection_schedule_test.cpp"],
    deps = [
        ":leader_selection_schedule",
        ":proposal_manager",
        "//common/test:test_main",
        "//platform/consensus/reputation:reputation_algorithm",
    ],
)

cc_test(
    name = "weight_update_controller_test",
    srcs = ["weight_update_controller_test.cpp"],
    deps = [
        ":leader_selection_schedule",
        ":weight_schedule",
        ":weight_update_controller",
        ":weight_update_experiment",
        "//common/crypto:mock_signature_verifier",
        "//common/test:test_main",
        "//platform/consensus/reputation:reputation_algorithm",
    ],
)

cc_test(
    name = "certified_weight_update_pipeline_test",
    srcs = ["certified_weight_update_pipeline_test.cpp"],
    deps = [
        ":certified_weight_update_pipeline",
        ":leader_selection_schedule",
        ":td_hotstuff_digest",
        ":weight_schedule",
        "//common/crypto:mock_signature_verifier",
        "//common/test:test_main",
        "//platform/consensus/reputation:reputation_algorithm",
    ],
)
