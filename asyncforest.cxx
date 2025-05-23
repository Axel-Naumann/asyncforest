/// Axel Naumann (axel@cern.ch), 2019-03-28
///

#include <array>
#include <chrono>
#include <future>
#include <iostream>
#include <random>
#include <thread>
#include <utility>
#include <vector>

////////////////////////////////////////////////////////////////////////////////
///
/// Mockup / demo / testbed for I/O scheduling.
///
/// Build as `clang++ -std=c++17 asyncforest.cxx`, or using cmake (FWIW).
///
/// # Design
///
/// - Data structures (in `Data`) simulate `TTree` / `RForest` data structures.
/// - Operations (in `Ops`) transform one to the others, e.g. remote data
/// transfer,
///   decompression and deserialization.
/// - An event loop iterates over the whole event data and wastes CPU, to
/// represent
///   analyses.
///
///
/// # What this allows to test
///
/// The operations can be scheduled synchronously: when a new `Buffer` is
/// needed, the next one is grabbed from the `Branch` and decompressed, then
/// things go on. Or, as done by the "managers" in `Axel`, everything is
/// connected by data dependencies modelled as `std::future`s. The use of
/// `std::future` is an implementation detail; the key ingredient here is there
/// fine-grained dependency specification - without synchronization points - and
/// the cost of double- buffering.
///
/// Other scheduling (e.g. what we do in `TTree`) can be added in parallel,
/// re-using `Data` and `Ops` to compare runtime behavior.
///
/// The context can be switched by selecting a given set of using declarations:
///     using IO_t = TRemoteIO;
///     //using IO_t = TLocalIO;
///     using Decompression_t = TDecompress;
///     //using Decompression_t = TUncompressed;
///     using Deserialization_t = TDeserialize;
///     //using Deserialization_t = TDeserializeTransparent;
///
/// Times can be tweaked, notably the per-event CPU time.
///
///
/// # Todo
///
/// Add jitter! Add more scheduling options (e.g. what `TTree` does)!
///
///
/// # Results
///
/// Throughput (entries/second) on Axel's laptop with 1s per entry processing
/// ("analysis") - for "skip2", only even entry numbers are "analyzed".
/// "Default" means TRemoteIO, TDecompress, TDeserialize.
/// "All sync no-ops" means the inverse: TLocalIO, TUncompressed,
/// TDeserializeTransparent, where everything is running on the main thread
/// (i.e. no threading involved).
///
/// nBranches:               10000 |  5
/// =====================================
/// Default                 | 0.07 | 0.96
/// ^ but skip2             | 0.07 | 1.78
/// TDeserializeTransparent | 0.23 | 0.97
/// ^ but skip2             | 0.26 | 1.94
/// All sync no-ops         | 1.01 | 1.00

/// Data mockup.
namespace Data {
constexpr const int kNumBranches = 100;

/// Possibly compressed bytes. Comes from raw storage.
struct Basket {
  Basket(float size, int entries)
      : fPossiblyCompressedSizeMB(size), fForestEntries(entries) {}
  const float fPossiblyCompressedSizeMB;
  const int fForestEntries;
};

/// Collection of `Basket`s for one column.
struct Branch {
  std::vector<Basket> fBaskets;
};

/// Collection of `Branch`es; granularity of I/O operations.
struct Cluster {
  static constexpr const float kPossiblyCompressedSizeMB = 30.;
  std::vector<Branch> fBranches;

  Cluster() {
    // Create branches. Sum of sizes needs to be kPossiblyCompressedSizeMB.
    float sumMB = 0;
    std::vector<std::vector<std::pair<float, int>>> sizeEntriesPattern{
        {{.3, 1},
         {.4, 1},
         {.3, 1},
         {.5, 2},
         {.6, 1},
         {.5, 2},
         {.3, 2}},                   // 2.9MB
        {{.1, 10}},                  // 0.1MB
        {{2., 10}},                  // 2.0MB
        {{22.8, 10}},                // 22.8MB
        {{1., 4}, {1., 5}, {0.2, 1}} // 2.2MB
    };

    for (int i = 0; i < kNumBranches - 1; ++i) {
      const auto &entry = sizeEntriesPattern[i % sizeEntriesPattern.size()];
      fBranches.emplace_back();
      for (auto &&[s, n] : entry) {
        float size = s / kNumBranches * kPossiblyCompressedSizeMB / 30.;
        sumMB += size;
        fBranches.back().fBaskets.emplace_back(size, n);
      }
    }
    if (sumMB > kPossiblyCompressedSizeMB) {
      std::cerr << "ERROR: we already have " << sumMB
                << "MB worth of branches, cannot add one more to reach "
                << kPossiblyCompressedSizeMB << "MB!\n";
      exit(1);
    }
    fBranches.emplace_back();
    fBranches.back().fBaskets.emplace_back(kPossiblyCompressedSizeMB - sumMB,
                                           10);
  }
};

/// Uncompressed bytes. Contains serialized objects. The result of
/// (possibly) decompressing a `Basket`.
struct Buffer {
  float fUncompressedSizeMB;
  int fForestEntries;
};

/// Mockup data model: four members, each stored in their own `Branch`.
struct Member {};
struct Event {
  std::array<Member, kNumBranches> members;
};
} // namespace Data

/// Data operations.
namespace Ops {

/// Waste some time without CPU usage to compare patterns.
/// Simulates e.g. I/O.
void Sleep(float sec) {
  using namespace std::chrono_literals;
  std::this_thread::sleep_for(sec * 1s);
}

/// Waste some time with CPU usage to compare patterns.
void WasteCPU(float sec) {
  using clock = std::chrono::high_resolution_clock;
  auto start = clock::now();
  std::mt19937_64 prng;
  while ((clock::now() - start).count() < sec * 1'000'000'000.) {
    for (int rep = 0; rep < 1000; rep++)
      prng();
  }
}

/// Call something asynchronously if it takes time, or synchronously if not.
template <class OP, class... ARGS>
auto AsyncOrNot(const OP &op, ARGS... args)
    -> decltype(std::async(op, args...)) {
  if (OP::kIsWorthATask) {
    // Simulate task scheduling overhead; Andrei says "less than 1 millisecond"
    WasteCPU(0.001);
    return std::async(op, args...);
  }

  return std::async(std::launch::deferred, op, args...);
}

/// Simulate grabbing a `Cluster` from remote.
struct TRemoteIO {
  static constexpr const bool kIsWorthATask = true;

  /// Simulate CPU behavior of remote I/O.
  Data::Cluster operator()() const {
    std::cout << "Doing TRemoteIO!\n";
    // About 100MB/s:
    Sleep(Data::Cluster::kPossiblyCompressedSizeMB / 100.);
    return {};
  }
};

/// Simulate grabbing a `Cluster` from local SSD or even persistent RAM.
struct TLocalIO {
  static constexpr const bool kIsWorthATask = false;

  /// Simulate CPU behavior of local I/O.
  Data::Cluster operator()() const {
    std::cout << "Doing TLocalIO!\n";
    // About 10G/s
    Sleep(Data::Cluster::kPossiblyCompressedSizeMB / 10000.);
    return {};
  }
};

/// Simulate decompressing a `Data::Basket`.
struct TDecompress {
  static constexpr const bool kIsWorthATask = true;

  /// Simulate CPU behavior of decompression.
  Data::Buffer operator()(const Data::Basket &basket) const {
    // std::cout << "Doing TDecompress!\n";
    //  Oksana estimates lz4 aim: 300..400 MB/s; lzma 10 MB/s
    WasteCPU(basket.fPossiblyCompressedSizeMB / 100.);
    return {basket.fPossiblyCompressedSizeMB * 3, basket.fForestEntries};
  }
};

/// Simulate no-op decompression on an uncompressed `Data::Basket`.
struct TUncompressed {
  static constexpr const bool kIsWorthATask = false;

  /// Simulate CPU behavior of already uncompressed `Data::Basket`s.
  Data::Buffer operator()(const Data::Basket &basket) const {
    // std::cout << "Doing TUncompressed!\n";
    return {basket.fPossiblyCompressedSizeMB, basket.fForestEntries};
  }
};

/// Simulate deserialization from a `Data::Buffer`.
struct TDeserialize {
  static constexpr const bool kIsWorthATask = true;

  /// Simulate conversion of a byte blob to objects.
  Data::Member operator()(const Data::Buffer &buf) const {
    // std::cout << "Doing TDeserialize!\n";
    WasteCPU(buf.fUncompressedSizeMB / 1000.);
    return {};
  }
};

/// Simulate no-op deserialization from a `Data::Buffer`, e.g. because the
/// data can be used as is (say `double[128]`).
struct TDeserializeTransparent {
  static constexpr const bool kIsWorthATask = false;

  // Simulate "deserialization" e.g. an array of floats into an array of floats
  // - no-op.
  Data::Member operator()(const Data::Buffer &) const {
    // std::cout << "Doing TDeserializeTrivial!\n";
    return {};
  }
};

/// Select a certain operation pattern:
using IO_t = TRemoteIO;
// using IO_t = TLocalIO;
using Decompression_t = TDecompress;
// using Decompression_t = TUncompressed;
using Deserialization_t = TDeserialize;
// using Deserialization_t = TDeserializeTransparent;

} // namespace Ops

/// Scheduling by chaining operations through `std::future`.
/// Possibly HPX-style.
namespace Futures {

/// Provides the current `Data::Cluster`, `async`-ing the next one.
struct ClusterManager {
  Data::Cluster fCurrent = Ops::IO_t()(); ///< Current cluster.
  std::atomic_int fClusterIdx = 0;        ///< Index of the current cluster.
  std::future<Data::Cluster> fNext{
      AsyncOrNot(Ops::IO_t())}; ///< Future on the next cluster.

  /// Move the next cluster to the current, start grabbing the next one,
  /// increment index.
  void Advance() {
    fCurrent = std::move(fNext.get());
    fNext = AsyncOrNot(Ops::IO_t());
  }

  /// If the index is larger than the current cluster index, `Advance()`.
  void PossiblyAdvance(int idx) {
    /// FIXME: this needs compare_exchange!
    int clusterIdx = fClusterIdx.load(std::memory_order_relaxed);
    if (clusterIdx >= idx)
      return;

    while (!fClusterIdx.compare_exchange_weak(clusterIdx, clusterIdx + 1,
                                              std::memory_order_release,
                                              std::memory_order_relaxed)) {
      if (fClusterIdx.load(std::memory_order_relaxed) == clusterIdx + 1)
        return;
    }
    Advance();
  }
};

/// Provides the current `Data::Buffer`, `async`-ing the next one.
struct BufferManager {
  int fBranchIdx;          ///< Index of the `Data::Branch` within
                           ///< `Data::Cluster::fBranches`.
  int fCurrentCluster = 0; ///< Current cluster index, so we can tell
                           ///< ClusterManager to read the next one (once).
  int fCurrentBasket = 0;  ///< Current basket index.
  int fCurrentEntry = 0;   ///< Current entry within `fCurrent`.
  Data::Buffer fCurrent;   ///< Currently active `Data::Buffer`.
  std::future<Data::Buffer> fNext; ///< Future on the next buffer.

  /// Construct from the branch index and the `ClusterManager`.
  BufferManager(int idx, ClusterManager &clusterMgr)
      : fBranchIdx(idx),
        fCurrent(Ops::Decompression_t()(GetCurrentBasket(clusterMgr))) {
    fNext = AsyncOrNot(Ops::Decompression_t(), GetCurrentBasket(clusterMgr));
  }

  /// Helper to get the `Data::Branch`.
  Data::Branch &GetBranch(ClusterManager &clusterMgr) {
    return clusterMgr.fCurrent.fBranches[fBranchIdx];
  }

  /// Helper to get the `Data::Basket`.
  Data::Basket &GetCurrentBasket(ClusterManager &clusterMgr) {
    return GetBranch(clusterMgr).fBaskets[fCurrentBasket];
  }

  /// Advance to next basket in `Data::Cluster`. This might advance the
  /// `Data::Cluster`.
  void Advance(ClusterManager &clusterMgr) {
    ++fCurrentBasket;
    fCurrent = std::move(fNext.get());
    if (fCurrentBasket == GetBranch(clusterMgr).fBaskets.size()) {
      // Need a new cluster.
      fCurrentBasket = 0;
      ++fCurrentCluster;
      clusterMgr.PossiblyAdvance(fCurrentCluster);
    }
    fNext = AsyncOrNot(Ops::Decompression_t(), GetCurrentBasket(clusterMgr));
  }

  /// Advance the entry inside `fCurrent`; might `Advance()` to `fNext`.
  void NextEntry(ClusterManager &clusterMgr) {
    ++fCurrentEntry;
    if (fCurrentEntry == fCurrent.fForestEntries) {
      fCurrentEntry = 0;
      Advance(clusterMgr);
    }
  }
};

/// Provides the current `Data::Event`, `async`-ing the next one.
struct EventManager {
  std::vector<BufferManager>
      fBufferMgrs;      ///< `BufferManager` for each branch / data member.
  Data::Event fCurrent; ///< Current deserialized event.
  std::future<Data::Event> fNext; ///< Future on next event.
  long long fEntry = 0;           ///< Current entry number.

  /// "Operation" to only assemble asynchronously if
  /// Deserialization_t::kIsWorthATask.
  struct AssembleOps {
    EventManager *fThis;
    ClusterManager &fClusterMgr;
    static constexpr const bool kIsWorthATask =
        Ops::Deserialization_t::kIsWorthATask;
    auto operator()() const { return fThis->Assemble(fClusterMgr); }
  };

  /// Construct from an `ClusterManager`. Initializes the `BufferManager`
  /// for each branch, and gets the first `Data::Event`.
  EventManager(ClusterManager &clusterMgr) {
    for (int i = 0; i < Data::kNumBranches; ++i)
      fBufferMgrs.emplace_back(i, clusterMgr);

    fCurrent = Assemble(clusterMgr);
    fNext = Ops::AsyncOrNot(AssembleOps{this, clusterMgr});
  }

  /// Create a `Data::Event` by deserializing its `Data::Members`.
  /// This might advance one or more `Data::Buffer`s.
  Data::Event Assemble(ClusterManager &clusterMgr) {
    // Get the next entry from each branch's buffer.
    // Advance to next buffer if needed.
    std::array<std::future<Data::Member>, Data::kNumBranches> futureMembers;
    int idx = 0;
    for (auto &bufMgr : fBufferMgrs)
      futureMembers[idx++] =
          Ops::AsyncOrNot(Ops::Deserialization_t(), bufMgr.fCurrent);

    // Assemble Event from deserialized Member-s.
    Data::Event ret;
    for (int i = 0; i < Data::kNumBranches; ++i)
      ret.members[i] = futureMembers[i].get();

    for (auto &bufMgr : fBufferMgrs)
      bufMgr.NextEntry(clusterMgr);
    return ret;
  }

  /// Advance to next event; start assembling the new next.
  void Advance(ClusterManager &clusterMgr) {
    ++fEntry;
    fCurrent = std::move(fNext.get());
    fNext = Ops::AsyncOrNot(AssembleOps{this, clusterMgr});
  }
};

/// Run on many entries, wasting CPU to simulate data processing / analysis.
int run() {
  ClusterManager clusterMgr;
  EventManager evtMgr(clusterMgr);
  using clock = std::chrono::high_resolution_clock;
  auto start = clock::now();
  while (true) {
    evtMgr.Advance(clusterMgr);
    // Process event data; 0.01s/event
    Ops::WasteCPU(0.01);

    // Process at least 20 entries.
    if (evtMgr.fEntry >= 20) {
      // But then stop after 20 seconds.
      double seconds = (clock::now() - start).count() / 1'000'000'000.;
      if (seconds > 20)
        break;
    }
  }
  std::cout << "Processed " << evtMgr.fEntry << " entries contained in "
            << clusterMgr.fClusterIdx + 1 << " clusters\n";
  return evtMgr.fEntry;
}
} // namespace Futures

#include <coroutine>
#include <exception>

/// Scheduling by chaining operations through coroutines.
namespace Coroutines {

// Adopted from Mateusz Fila!

namespace Internal {

// Coroutine that can be nested and co_awaited on inside another coroutine.
// Doesn't return a value, doesn't yield. Rethrows exceptions on resume.
// The outer coroutine should be resumed from outside.
template <class T>
class [[nodiscard]] NestableTask {
public:
    struct promise_type;  // typedef required by coroutines
    using handle_type =
        std::coroutine_handle<promise_type>;  // not required but useful

    NestableTask(handle_type coroutine_handle)
        : m_coroutine(coroutine_handle) {}  // required by coroutines
    ~NestableTask() {
        if (m_coroutine) {
            m_coroutine.destroy();
        }
    }
    NestableTask() = default;
    NestableTask(const NestableTask&) = delete;
    NestableTask& operator=(const NestableTask&) = delete;
    NestableTask(NestableTask&& other) noexcept
        : m_coroutine{other.m_coroutine} {
        other.m_coroutine = {};
    }
    NestableTask& operator=(NestableTask&& other) noexcept {
        if (this != &other) {
            if (m_coroutine) {
                m_coroutine.destroy();
            }
            m_coroutine = other.m_coroutine;
            other.m_coroutine = {};
        }
        return *this;
    }
    // resume the coroutine from outside
    inline void resume() const;
    // check if finished from outside
    inline bool done() const { return m_coroutine.done(); }

    // awaitable interface
    // don't skip suspensions
    bool await_ready() const noexcept { return false; }
    // when awaited, store the parent coroutine handle and resume this coroutine
    inline auto await_suspend(handle_type handle) noexcept;
    // nothing special on resume, doesn't produce a value
    void await_resume() const noexcept {}

    Data::Event get() const {
      if (m_coroutine) {
        if (!m_coroutine.done()) {
          // m_coroutine.resume();
          throw std::runtime_error("Coroutine not finished");
        }
        if (m_coroutine.done()) {
          return m_coroutine.promise().m_value;
        }
        throw std::runtime_error("Coroutine still not finished");
      }
      throw std::runtime_error("Invalid coroutine!");
      return {};
    }

    private:
    handle_type m_coroutine;
};

template <class T>
struct NestableTask<T>::promise_type {
    // storage for exceptions thrown in the coroutine
    std::exception_ptr m_exception;
    // handle to the parent coroutine if the coroutine has one
    handle_type m_parent;
    // Value
    T m_value;

    // required by coroutines
    NestableTask get_return_object() {
        return {NestableTask::handle_type::from_promise(*this)};
    }
    // called on coroutine start
    std::suspend_always initial_suspend() const { return {}; }
    // called on coroutine completion
    // on final_suspend return control to the parent coroutine
    auto final_suspend() const noexcept {
        struct final_awaiter {
            // don't skip suspensions
            bool await_ready() const noexcept { return false; }
            // resume the parent of the suspended coroutine if it has one
            // or if not then continue control to the caller
            std::coroutine_handle<> await_suspend(handle_type handle) noexcept {
                auto parent = handle.promise().m_parent;
                if (parent) {
                    return parent;
                }
                return std::noop_coroutine();
            }
            // nothing special on resume, doesn't produce a value
            void await_resume() const noexcept {}
        };
        return final_awaiter{};
    }
    // acts as a catch block for exceptions thrown in the coroutine
    void unhandled_exception() { m_exception = std::current_exception(); }
    // called on (implicit or explicit) co_return or co_return void
    void return_value(Data::Event&& ev) { m_value = std::move(ev); }
};

template <class T>
inline void NestableTask<T>::resume() const {
    if (!m_coroutine.done()) {
        m_coroutine.resume();
    }
    if (m_coroutine.promise().m_exception) {
        std::rethrow_exception(m_coroutine.promise().m_exception);
    }
}

template <class T>
inline auto NestableTask<T>::await_suspend(handle_type handle) noexcept {
    m_coroutine.promise().m_parent = handle;
    return m_coroutine;
}

} // namespace Internal

/// Provides the current `Data::Cluster`, `async`-ing the next one.
struct ClusterManager {
  Data::Cluster fCurrent = Ops::IO_t()(); ///< Current cluster.
  std::atomic_int fClusterIdx = 0;        ///< Index of the current cluster.
  std::future<Data::Cluster> fNext{
      AsyncOrNot(Ops::IO_t())}; ///< Future on the next cluster.

  /// Move the next cluster to the current, start grabbing the next one,
  /// increment index.
  void Advance() {
    fCurrent = std::move(fNext.get());
    fNext = AsyncOrNot(Ops::IO_t());
  }

  /// If the index is larger than the current cluster index, `Advance()`.
  void PossiblyAdvance(int idx) {
    /// FIXME: this needs compare_exchange!
    int clusterIdx = fClusterIdx.load(std::memory_order_relaxed);
    if (clusterIdx >= idx)
      return;

    while (!fClusterIdx.compare_exchange_weak(clusterIdx, clusterIdx + 1,
                                              std::memory_order_release,
                                              std::memory_order_relaxed)) {
      if (fClusterIdx.load(std::memory_order_relaxed) == clusterIdx + 1)
        return;
    }
    Advance();
  }
};

/// Provides the current `Data::Buffer`, `async`-ing the next one.
struct BufferManager {
  int fBranchIdx;          ///< Index of the `Data::Branch` within
                           ///< `Data::Cluster::fBranches`.
  int fCurrentCluster = 0; ///< Current cluster index, so we can tell
                           ///< ClusterManager to read the next one (once).
  int fCurrentBasket = 0;  ///< Current basket index.
  int fCurrentEntry = 0;   ///< Current entry within `fCurrent`.
  Data::Buffer fCurrent;   ///< Currently active `Data::Buffer`.
  std::future<Data::Buffer> fNext; ///< Future on the next buffer.

  /// Construct from the branch index and the `ClusterManager`.
  BufferManager(int idx, ClusterManager &clusterMgr)
      : fBranchIdx(idx),
        fCurrent(Ops::Decompression_t()(GetCurrentBasket(clusterMgr))) {
    fNext = AsyncOrNot(Ops::Decompression_t(), GetCurrentBasket(clusterMgr));
  }

  /// Helper to get the `Data::Branch`.
  Data::Branch &GetBranch(ClusterManager &clusterMgr) {
    return clusterMgr.fCurrent.fBranches[fBranchIdx];
  }

  /// Helper to get the `Data::Basket`.
  Data::Basket &GetCurrentBasket(ClusterManager &clusterMgr) {
    return GetBranch(clusterMgr).fBaskets[fCurrentBasket];
  }

  /// Advance to next basket in `Data::Cluster`. This might advance the
  /// `Data::Cluster`.
  void Advance(ClusterManager &clusterMgr) {
    ++fCurrentBasket;
    fCurrent = std::move(fNext.get());
    if (fCurrentBasket == GetBranch(clusterMgr).fBaskets.size()) {
      // Need a new cluster.
      fCurrentBasket = 0;
      ++fCurrentCluster;
      clusterMgr.PossiblyAdvance(fCurrentCluster);
    }
    fNext = AsyncOrNot(Ops::Decompression_t(), GetCurrentBasket(clusterMgr));
  }

  /// Advance the entry inside `fCurrent`; might `Advance()` to `fNext`.
  void NextEntry(ClusterManager &clusterMgr) {
    ++fCurrentEntry;
    if (fCurrentEntry == fCurrent.fForestEntries) {
      fCurrentEntry = 0;
      Advance(clusterMgr);
    }
  }
};

/// Provides the current `Data::Event`, `async`-ing the next one.
struct EventManager {
  std::vector<BufferManager>
      fBufferMgrs;      ///< `BufferManager` for each branch / data member.
  Data::Event fCurrent; ///< Current deserialized event.
  Internal::NestableTask<Data::Event> fNext; ///< Promise on next event.
  long long fEntry = 0;           ///< Current entry number.

  /// "Operation" to only assemble asynchronously if
  /// Deserialization_t::kIsWorthATask.
  struct AssembleOps {
    EventManager *fThis;
    ClusterManager &fClusterMgr;
    static constexpr const bool kIsWorthATask =
        Ops::Deserialization_t::kIsWorthATask;
    auto operator()() const { return fThis->Assemble(fClusterMgr); }
  };

  /// Construct from an `ClusterManager`. Initializes the `BufferManager`
  /// for each branch, and gets the first `Data::Event`.
  EventManager(ClusterManager &clusterMgr) {
    for (int i = 0; i < Data::kNumBranches; ++i)
      fBufferMgrs.emplace_back(i, clusterMgr);

    auto coCurrent = Assemble(clusterMgr);
    fCurrent = coCurrent.get();
    fNext = AssembleOps{this, clusterMgr}();
  }

  /// Create a `Data::Event` by deserializing its `Data::Members`.
  /// This might advance one or more `Data::Buffer`s.
  Internal::NestableTask<Data::Event> Assemble(ClusterManager &clusterMgr) {
    // Get the next entry from each branch's buffer.
    // Advance to next buffer if needed.
    std::array<std::future<Data::Member>, Data::kNumBranches> futureMembers;
    int idx = 0;
    for (auto &bufMgr : fBufferMgrs)
      futureMembers[idx++] =
          Ops::AsyncOrNot(Ops::Deserialization_t(), bufMgr.fCurrent);

    // Assemble Event from deserialized Member-s.
    Data::Event ret;
    for (int i = 0; i < Data::kNumBranches; ++i)
      ret.members[i] = futureMembers[i].get();

    for (auto &bufMgr : fBufferMgrs)
      bufMgr.NextEntry(clusterMgr);
    co_return ret;
  }

  /// Advance to next event; start assembling the new next.
  void Advance(ClusterManager &clusterMgr) {
    ++fEntry;
    fCurrent = std::move(fNext.get());
    fNext = AssembleOps{this, clusterMgr}();
  }
};

/// Run on many entries, wasting CPU to simulate data processing / analysis.
int run() {
  ClusterManager clusterMgr;
  EventManager evtMgr(clusterMgr);
  using clock = std::chrono::high_resolution_clock;
  auto start = clock::now();
  while (true) {
    evtMgr.Advance(clusterMgr);
    // Process event data; 0.01s/event
    Ops::WasteCPU(0.01);

    // Process at least 20 entries.
    if (evtMgr.fEntry >= 20) {
      // But then stop after 20 seconds.
      double seconds = (clock::now() - start).count() / 1'000'000'000.;
      if (seconds > 20)
        break;
    }
  }
  std::cout << "Processed " << evtMgr.fEntry << " entries contained in "
            << clusterMgr.fClusterIdx + 1 << " clusters\n";
  return evtMgr.fEntry;
}
} // namespace Coroutines

/// Helper function to time `func`.
template <class FUNC> void time(FUNC &func) {
  using clock = std::chrono::high_resolution_clock;
  auto start = clock::now();
  int nEntries = func();
  double seconds = (clock::now() - start).count() / 1'000'000'000.;
  std::cout << nEntries / seconds << " entries/s == " << seconds / nEntries
            << "s/Entry\n";
}

/// Time the different scheduling options.
int main() {
  //time(Futures::run);
  time(Coroutines::run);
  return 0;
}
