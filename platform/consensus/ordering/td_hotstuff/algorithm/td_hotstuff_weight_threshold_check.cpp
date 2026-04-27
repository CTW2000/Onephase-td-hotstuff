#include "platform/consensus/ordering/td_hotstuff/algorithm/td_hotstuff.h"

#include <cstdlib>
#include <iostream>
#include <vector>

namespace {

void ExpectEq(int actual, int expected, const char* label) {
  if (actual != expected) {
    std::cerr << label << ": expected " << expected << ", got " << actual << std::endl;
    std::exit(1);
  }
}

}  // namespace

int main() {
  using resdb::td_hotstuff::ComputeWeightThreshold;

  ExpectEq(ComputeWeightThreshold(1, std::vector<int>(5, 1)), 3, "uniform n=5");
  ExpectEq(ComputeWeightThreshold(4, std::vector<int>(15, 1)), 9, "uniform n=15");
  ExpectEq(ComputeWeightThreshold(6, std::vector<int>(20, 1)), 13, "uniform n=20");
  ExpectEq(ComputeWeightThreshold(1, std::vector<int>({1, 3, 1, 3, 2})), 6,
           "weighted n=5");
  ExpectEq(ComputeWeightThreshold(6, std::vector<int>(20, 2)), 26, "weighted n=20");

  return 0;
}
