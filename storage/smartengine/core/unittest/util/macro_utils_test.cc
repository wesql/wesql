#include "util/macro_utils.h"
#include <atomic>
#include <thread>
#include <chrono>
#include <cstring>
#include <errno.h>
#include <sys/syscall.h>
#include <linux/futex.h>
#include "util/testharness.h"


namespace
{

// Test UNUSED macro
TEST(MacroTest, TestUnused)
{
  int unused_var = 42;
  UNUSED(unused_var); // Ensure UNUSED compiles successfully
  SUCCEED();
}

// Test LIKELY and UNLIKELY macros
TEST(MacroTest, TestLikelyUnlikely)
{
  int x = 10;
  bool result = LIKELY(x > 5);
  EXPECT_TRUE(result);
  result = UNLIKELY(x < 5);
  EXPECT_FALSE(result);
}

// Test IS_FALSE macro
TEST(MacroTest, TestIsFalse)
{
  bool expr = false;
  EXPECT_TRUE(IS_FALSE(expr));
  expr = true;
  EXPECT_FALSE(IS_FALSE(expr));
}

// Test IS_NULL and IS_NOTNULL macros
TEST(MacroTest, TestNullMacros)
{
  int* null_ptr = nullptr;
  int value = 1;
  int* non_null_ptr = &value;

  EXPECT_TRUE(IS_NULL(null_ptr));
  EXPECT_FALSE(IS_NOTNULL(null_ptr));
  EXPECT_FALSE(IS_NULL(non_null_ptr));
  EXPECT_TRUE(IS_NOTNULL(non_null_ptr));
}

// Mock function to simulate behavior for SUCCED and FAILED macros
int mock_function_success()
{
  return smartengine::common::Status::kOk;
}
int mock_function_fail()
{
  return smartengine::common::Status::kCorruption;
}
TEST(MacroTest, TestSucceedFailed)
{
  int ret = smartengine::common::Status::kOk;
  EXPECT_TRUE(SUCCED(mock_function_success()));
  EXPECT_FALSE(SUCCED(mock_function_fail()));
  EXPECT_FALSE(FAILED(mock_function_success()));
  EXPECT_TRUE(FAILED(mock_function_fail()));
}

// Test CACHE_ALIGNED macro
struct CACHE_ALIGNED AlignedStruct
{
    int data[16];
};
TEST(MacroTest, TestCacheAligned)
{
  AlignedStruct obj;
  EXPECT_EQ(reinterpret_cast<uintptr_t>(&obj) % CACHE_LINE_SIZE, 0);
}

// Test PAUSE macro
TEST(MacroTest, TestPause)
{
#if defined(PAUSE)
    PAUSE(); // Verify the macro runs without errors
    SUCCEED();
#else
    GTEST_SKIP() << "PAUSE macro is not defined for this architecture.";
#endif
}


// Unit tests for atomic macros
TEST(AtomicMacrosTest, TestAtomicMacros)
{
  int value = 42;
  int cmp = 42;
  int newv = 84;

  // Test ATOMIC_LOAD
  int loaded_value = ATOMIC_LOAD(&value);
  EXPECT_EQ(loaded_value, 42);

  // Test ATOMIC_BCAS
  bool cas_success = ATOMIC_BCAS(&value, cmp, newv);
  EXPECT_TRUE(cas_success);
  EXPECT_EQ(value, 84);

  // Test ATOMIC_AND_FETCH
  int and_result = ATOMIC_AND_FETCH(&value, 0xF);
  EXPECT_EQ(and_result, 4);

  // Test ATOMIC_SUB_FETCH
  int sub_result = ATOMIC_SUB_FETCH(&value, 1);
  EXPECT_EQ(sub_result, 3);

  // Test ATOMIC_FETCH_ADD
  int fetch_add_result = ATOMIC_FETCH_ADD(&value, 10);
  EXPECT_EQ(fetch_add_result, 3);  // Returns the old value
  EXPECT_EQ(value, 13);           // New value after addition

  // Test ATOMIC_ADD_FETCH
  int add_fetch_result = ATOMIC_ADD_FETCH(&value, 5);
  EXPECT_EQ(add_fetch_result, 18); // Returns the new value

  // Test ATOMIC_SET
  int old_value = ATOMIC_SET(&value, 100);
  EXPECT_EQ(old_value, 18);
  EXPECT_EQ(value, 100);

  // Test ATOMIC_FETCH_AND
  int fetch_and_result = ATOMIC_FETCH_AND(&value, 0xF);
  EXPECT_EQ(fetch_and_result, 100);
  EXPECT_EQ(value, 4);

  // Test ATOMIC_CAS
  bool cas_result = ATOMIC_CAS(&value, 4, 50);
  EXPECT_TRUE(cas_result);
  EXPECT_EQ(value, 50);
}

} // namespace

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
	smartengine::util::test::init_logger(__FILE__);
  return RUN_ALL_TESTS();
}
