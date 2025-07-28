#!/usr/bin/env python3
"""
Test runner for Snowflake connection tests
"""
import subprocess
import sys
import os

def run_tests():
    """Run the test suite"""
    print("🧪 Running Snowflake Connection Tests...")
    print("=" * 50)
    
    # Run unit tests
    print("\n📋 Running Unit Tests...")
    result = subprocess.run([
        sys.executable, "-m", "pytest", 
        "tests/test_snowflake_connection.py", 
        "-v", 
        "-m", "not integration"
    ], capture_output=True, text=True)
    
    print(result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)
    
    if result.returncode != 0:
        print(f"❌ Unit tests failed with return code: {result.returncode}")
        return False
    
    # Run integration tests if environment variable is set
    if os.getenv("SNOWFLAKE_INTEGRATION_TEST"):
        print("\n🔗 Running Integration Tests...")
        result = subprocess.run([
            sys.executable, "-m", "pytest", 
            "tests/test_snowflake_connection.py::TestSnowflakeConnection::test_snowflake_connection_integration", 
            "-v"
        ], capture_output=True, text=True)
        
        print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)
        
        if result.returncode != 0:
            print(f"❌ Integration tests failed with return code: {result.returncode}")
            return False
    else:
        print("\n⏭️  Skipping integration tests. Set SNOWFLAKE_INTEGRATION_TEST=1 to run them.")
    
    print("\n✅ All tests completed successfully!")
    return True

def run_startup_test():
    """Test the application startup with Snowflake connection"""
    print("\n🚀 Testing Application Startup...")
    print("=" * 50)
    
    # Import and test the startup function
    try:
        from app.main import startup_event
        import asyncio
        
        # Run the startup event
        asyncio.run(startup_event())
        print("✅ Application startup test completed successfully!")
        return True
    except Exception as e:
        print(f"❌ Application startup test failed: {str(e)}")
        return False

if __name__ == "__main__":
    print("Snowflake Connection Test Suite")
    print("=" * 50)
    
    # Check if we're in the right directory
    if not os.path.exists("app"):
        print("❌ Error: Please run this script from the project root directory")
        sys.exit(1)
    
    # Run tests
    tests_passed = run_tests()
    
    # Run startup test
    startup_passed = run_startup_test()
    
    if tests_passed and startup_passed:
        print("\n🎉 All tests passed!")
        sys.exit(0)
    else:
        print("\n💥 Some tests failed!")
        sys.exit(1) 