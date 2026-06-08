#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "platform/consensus/reputation/reputation_roots.h"
#include "platform/consensus/reputation/reputation_types.h"
#include "platform/consensus/reputation/reputation_utils.h"
#include "platform/consensus/reputation/strong_fault_detector.h"

namespace resdb {
namespace consensus {
namespace reputation {

ReputationCandidate ComputeReputationCandidate(
    const ReputationWindowInput& input, const ReputationConfig& config);

}  // namespace reputation
}  // namespace consensus
}  // namespace resdb
