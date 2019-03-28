#include <array>
#include <chrono>
#include <future>
#include <iostream>
#include <random>
#include <vector>

/// Data mockup.
namespace Data {
   struct Basket {
      const float fPossiblyCompressedSizeMB;
      const int fForestEntries;
   };

   struct Branch {
      Branch(std::initializer_list<Basket> il): fBaskets{il} {}
      Branch(const Branch&) = default;
      Branch(Branch&&) = default;
      std::vector<Basket> fBaskets;
   };

   struct Cluster {
      static constexpr const float kPossiblyCompressedSizeMB = 30.;
      std::vector<Branch> fBranches;

      Cluster():
         fBranches{
            {{.3, 1}, {.4, 1}, {.3, 1}, {.5, 2}, {.3, 1}, {.5, 2}, {.4, 2}},
            {{1., 10}},
            {{2., 10}},
            {{1., 4}, {1., 5}, {0.2, 1}}
         }
      {}
   };

   struct Buffer {
      float fUncompressedSizeMB;
      int fForestEntries;
   };

   struct Member {};
   struct Event {
      Member m1;
      Member m2;
      Member m3;
      Member m4;
   };
}

// Data operations.
namespace Ops {
void Sleep(float sec)
{
   // Waste some time without CPU usage to compare patterns.
   // Simulates e.g. I/O.
   using namespace std::chrono_literals;
   std::this_thread::sleep_for(sec * 1s);
} 

void WasteCPU(float sec)
{
   // Waste some time with CPU usage to compare patterns.
   using clock = std::chrono::high_resolution_clock;
  	auto start = clock::now();
   std::mt19937_64 prng;
   while((clock::now() - start).count() < sec * 1'000'000'000.) {
      for (int rep = 0; rep < 1'000'000; rep++)
         prng();
   }
} 

template <class OP, class FUNC, class... ARGS>
auto AsyncOrNot(FUNC func, ARGS... args) {
   constexpr const std::launch launchDefault
      = std::launch::async | std::launch::deferred;

   return std::async(OP::kIsWorthATask ? launchDefault : std::launch{}, func, args...);
}

struct TRemoteIO {
   static constexpr const bool kIsWorthATask = true;

   Data::Cluster operator()()
   {
      // Perform e.g. remote I/O
      std::cout << "Doing TRemoteIO!\n";
      // About 100MB/s:
      Sleep(Data::Cluster::kPossiblyCompressedSizeMB / 100.);
      return {};
   }
};

struct TLocalIO {
   static constexpr const bool kIsWorthATask = false;

   Data::Cluster operator()()
   {
      // Perform fast I/O
      std::cout << "Doing TLocalIO!\n";
      // About 10G/s
      Sleep(Data::Cluster::kPossiblyCompressedSizeMB / 10000.);
      return {};
   }
};

struct TDecompress {
   static constexpr const bool kIsWorthATask = true;

   Data::Buffer operator()(const Data::Basket &basket)
   {
      // LZMA etc
      //std::cout << "Doing TDecompress!\n";
      // Oksana estimates lz4 aim: 300..400 MB/s; lzma 10 MB/s
      WasteCPU(basket.fPossiblyCompressedSizeMB / 100.);
      return {basket.fPossiblyCompressedSizeMB * 3, basket.fForestEntries};
   }
};

struct TUncompressed {
   static constexpr const bool kIsWorthATask = false;

   Data::Buffer operator()(const Data::Basket &basket)
   {
      // Uncompressed file
      //std::cout << "Doing TUncompressed!\n";
      return {basket.fPossiblyCompressedSizeMB, basket.fForestEntries};
   }
};

struct TDeserialize {
   static constexpr const bool kIsWorthATask = true;

   Data::Member operator()(const Data::Buffer &buf)
   {
      // Convert byte blob to objects
      //std::cout << "Doing TDeserialize!\n";
      WasteCPU(buf.fUncompressedSizeMB / 1000.);
      return {};
   }
};

struct TDeserializeTrivial {
   static constexpr const bool kIsWorthATask = false;

   Data::Member operator()(const Data::Buffer &)
   {
      // We "deserialize" e.g. an array of floats into an array of floats - no-op.
      //std::cout << "Doing TDeserializeTrivial!\n";
      return {};
   }
};

/// Select a certain operation pattern:
using IO_t = TRemoteIO;
using Decompression_t = TDecompress;
using Deserialization_t = TDeserialize; 

} // namespace Ops

#include <future>
namespace Axel {
   struct ClusterManager {
      Data::Cluster fCurrent = Ops::IO_t()();
      int fClusterIdx = 0;
      std::future<Data::Cluster> fNext{std::async(Ops::IO_t())};
      static std::atomic_int fgNum;

      void Advance()
      {
         ++fClusterIdx;
         fCurrent = std::move(fNext.get());
         fNext = std::async(Ops::IO_t());
      }

      void PossiblyAdvance(int idx)
      {
         if (fClusterIdx < idx)
            Advance();
      }
   };

   std::atomic_int ClusterManager::fgNum{};

   struct BufferManager {
      int fBranchIdx;
      int fCurrentCluster = 0;
      int fCurrentBasket = 0;
      int fCurrentEntry = 0;
      Data::Buffer fCurrent;
      std::future<Data::Buffer> fNext;

      auto &GetCurrentBasket(ClusterManager &clusterMgr)
      {
         return GetBranch(clusterMgr).fBaskets[fCurrentBasket];
      }

      BufferManager(int idx, ClusterManager &clusterMgr):
      fBranchIdx(idx), fCurrent(Ops::Decompression_t()(GetCurrentBasket(clusterMgr)))
      {
         fNext = std::async(Ops::Decompression_t(), GetCurrentBasket(clusterMgr));
      }

      Data::Branch &GetBranch(ClusterManager &clusterMgr)
      {
         return clusterMgr.fCurrent.fBranches[fBranchIdx];
      }

      void Advance(ClusterManager &clusterMgr)
      {
         ++fCurrentBasket;
         fCurrent = std::move(fNext.get());
         if (fCurrentBasket == GetBranch(clusterMgr).fBaskets.size()) {
            // Need a new cluster.
            fCurrentBasket = 0;
            ++fCurrentCluster;
            clusterMgr.PossiblyAdvance(fCurrentCluster);
         } 
         fNext = std::async(Ops::Decompression_t(), GetCurrentBasket(clusterMgr));
      }

      void NextEntry(ClusterManager &clusterMgr)
      {
         ++fCurrentEntry;
         if (fCurrentEntry == fCurrent.fForestEntries) {
            fCurrentEntry = 0;
            Advance(clusterMgr);
         }
      }
   };

   struct EventManager {
      std::vector<BufferManager> fBufferMgrs;
      Data::Event fCurrent;
      std::future<Data::Event> fNext;
      long long fEntry = 0;

      EventManager(ClusterManager& clusterMgr)
      {
         fBufferMgrs.emplace_back(0, clusterMgr);
         fBufferMgrs.emplace_back(1, clusterMgr);
         fBufferMgrs.emplace_back(2, clusterMgr);
         fBufferMgrs.emplace_back(3, clusterMgr);
         fCurrent = Assemble(clusterMgr);
         fNext = std::async([this, &clusterMgr] {return Assemble(clusterMgr);});
      }

      Data::Event Assemble(ClusterManager &clusterMgr)
      {
         // Get the next entry from each branch's buffer.
         // Advance to next buffer if needed.
         std::array<std::future<Data::Member>, 4> futureMembers;
         int idx = 0;
         for (auto &bufMgr: fBufferMgrs) {
            bufMgr.NextEntry(clusterMgr);
            futureMembers[idx++] = std::async(Ops::Deserialization_t(), bufMgr.fCurrent);
         }
         // Assemble Event from deserialized Member-s.
         return {futureMembers[0].get(), futureMembers[1].get(), futureMembers[2].get(), futureMembers[3].get()};
      }

      void Advance(ClusterManager &clusterMgr)
      {
         ++fEntry;
         fCurrent = std::move(fNext.get());
         fNext = std::async([this, &clusterMgr] {return Assemble(clusterMgr);});
      }
   };

   void run()
   {
      ClusterManager clusterMgr;
      EventManager evtMgr(clusterMgr);
      for (int entry = 0; entry < 1'000; entry++) {
         evtMgr.Advance(clusterMgr);
         // Process event data; 0.1s/event
         Ops::WasteCPU(0.01);
      }
      std::cout << "Processed " << clusterMgr.fgNum << "clusters containing " << evtMgr.fEntry << " entries\n";
   }
} // namespace Axel

template <class FUNC>
void time(FUNC &func)
{
   using clock = std::chrono::high_resolution_clock;
   auto start = clock::now();
   func();
   std::cout << (clock::now() - start).count() << "ns\n";
}

int main()
{
   time(Axel::run);
   return 0;
}