#pragma once

#include <cstdint>
#include <map>
#include <vector>

namespace resdb {
namespace td_hotstuff {

struct QcSignerInfo {
  int signer = 0;
  int64_t weight = 0;
};

struct QcSignerDiversityConfig {
  bool enabled = false;
  uint64_t cooldown_rounds = 1;
};

QcSignerDiversityConfig QcSignerDiversityConfigFromEnv();

class QcSignerCooldownTracker {
 public:
  QcSignerCooldownTracker() = default;
  explicit QcSignerCooldownTracker(QcSignerDiversityConfig config);

  bool enabled() const { return config_.enabled; }

  std::vector<int> SelectSignersForQc(
      const std::vector<QcSignerInfo>& available_signers,
      int64_t quorum_weight, uint64_t fairness_seed = 0);
  void RecordQcSigners(const std::vector<int>& signers);

 private:
  bool IsCooling(int signer) const;
  uint64_t SelectionDebt(int signer) const;

  QcSignerDiversityConfig config_;
  uint64_t qc_sequence_ = 0;
  std::map<int, uint64_t> last_included_sequence_by_signer_;
  std::map<int, uint64_t> eligible_count_by_signer_;
  std::map<int, uint64_t> included_count_by_signer_;
};

}  // namespace td_hotstuff
}  // namespace resdb
