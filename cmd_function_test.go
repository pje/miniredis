package miniredis

import (
	"testing"

	"github.com/alicebob/miniredis/v2/proto"
)

// Test FUNCTION LOAD and FCALL
func TestFunctionLoad(t *testing.T) {
	_, c := runWithClient(t)

	// Test loading a simple function
	script := "#!lua name=mylib\nredis.register_function('myfunc', function(keys, args) return 'hello' end)"
	mustDo(t, c,
		"FUNCTION", "LOAD", script,
		proto.String("mylib"),
	)

	// Test FCALL
	mustDo(t, c,
		"FCALL", "myfunc", "0",
		proto.String("hello"),
	)
}

// Test FUNCTION LIST
func TestFunctionList(t *testing.T) {
	_, c := runWithClient(t)

	// No functions initially
	mustDo(t, c,
		"FUNCTION", "LIST",
		proto.Array(),
	)

	// Load a function
	script := "#!lua name=mylib\nredis.register_function('myfunc', function(keys, args) return 'hello' end)"
	mustDo(t, c,
		"FUNCTION", "LOAD", script,
		proto.String("mylib"),
	)

	// Verify list contains the function
	resp, err := c.Do("FUNCTION", "LIST")
	if err != nil {
		t.Fatal(err)
	}

	result, err := proto.ReadArray(resp)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 1 {
		t.Fatalf("expected 1 library, got %d", len(result))
	}

	// Parse the library information
	libInfo, err := proto.ReadArray(result[0])
	if err != nil {
		t.Fatal(err)
	}

	// Check library name field (should be at index 1)
	libraryName, err := proto.ReadString(libInfo[1])
	if err != nil {
		t.Fatal(err)
	}

	if libraryName != "mylib" {
		t.Fatalf("expected library name to be 'mylib', got %s", libraryName)
	}
}

// Test FCALL with keys and arguments
func TestFunctionCallWithArgs(t *testing.T) {
	_, c := runWithClient(t)

	// Load a function that uses keys and args
	script := `#!lua name=mylib
redis.register_function('set_get', function(keys, args)
  redis.call('SET', keys[1], args[1])
  return redis.call('GET', keys[1])
end)`

	mustDo(t, c,
		"FUNCTION", "LOAD", script,
		proto.String("mylib"),
	)

	// Call the function with a key and argument
	mustDo(t, c,
		"FCALL", "set_get", "1", "mykey", "myvalue",
		proto.String("myvalue"),
	)

	// Verify the key was set
	mustDo(t, c,
		"GET", "mykey",
		proto.String("myvalue"),
	)
}

// Test FUNCTION DELETE
func TestFunctionDelete(t *testing.T) {
	_, c := runWithClient(t)

	// Load a library
	script := "#!lua name=mylib\nredis.register_function('myfunc', function(keys, args) return 'hello' end)"
	mustDo(t, c,
		"FUNCTION", "LOAD", script,
		proto.String("mylib"),
	)

	// Delete the library
	mustDo(t, c,
		"FUNCTION", "DELETE", "mylib",
		proto.String("OK"),
	)

	// Verify it's gone
	mustDo(t, c,
		"FUNCTION", "LIST",
		proto.Array(),
	)

	// Try to call a deleted function
	mustDo(t, c,
		"FCALL", "myfunc", "0",
		proto.Error("ERR Function not found"),
	)
}

// Test FUNCTION FLUSH
func TestFunctionFlush(t *testing.T) {
	_, c := runWithClient(t)

	// Load two libraries
	script1 := "#!lua name=mylib1\nredis.register_function('myfunc1', function(keys, args) return 'hello1' end)"
	mustDo(t, c,
		"FUNCTION", "LOAD", script1,
		proto.String("mylib1"),
	)

	script2 := "#!lua name=mylib2\nredis.register_function('myfunc2', function(keys, args) return 'hello2' end)"
	mustDo(t, c,
		"FUNCTION", "LOAD", script2,
		proto.String("mylib2"),
	)

	// Flush all functions
	mustDo(t, c,
		"FUNCTION", "FLUSH",
		proto.String("OK"),
	)

	// Verify all functions are gone
	mustDo(t, c,
		"FUNCTION", "LIST",
		proto.Array(),
	)
}

// Test FCALL_RO with a read-only function
func TestFunctionCallReadOnly(t *testing.T) {
	_, c := runWithClient(t)

	// Load a library with a read-only function
	script := `#!lua name=mylib
redis.register_function{
  function_name='readonly_func',
  callback=function(keys, args) return redis.call('GET', keys[1]) end,
  flags={'no-writes'}
}`

	mustDo(t, c,
		"FUNCTION", "LOAD", script,
		proto.String("mylib"),
	)

	// Set a key for testing
	mustDo(t, c,
		"SET", "testkey", "testvalue",
		proto.String("OK"),
	)

	// Call with FCALL_RO
	mustDo(t, c,
		"FCALL_RO", "readonly_func", "1", "testkey",
		proto.String("testvalue"),
	)

	// Test that a write function fails with FCALL_RO
	script2 := `#!lua name=writelib
redis.register_function('write_func', function(keys, args)
  return redis.call('SET', keys[1], args[1])
end)`

	mustDo(t, c,
		"FUNCTION", "LOAD", script2,
		proto.String("writelib"),
	)

	mustDo(t, c,
		"FCALL_RO", "write_func", "1", "testkey", "newvalue",
		proto.Error("ERR Can't execute a function with write flag using FCALL_RO"),
	)
}

// Test FUNCTION DUMP and RESTORE
func TestFunctionDumpRestore(t *testing.T) {
	_, c := runWithClient(t)

	// Load a function
	script := "#!lua name=mylib\nredis.register_function('myfunc', function(keys, args) return 'hello' end)"
	mustDo(t, c,
		"FUNCTION", "LOAD", script,
		proto.String("mylib"),
	)

	// Dump all functions
	dump, err := c.Do("FUNCTION", "DUMP")
	if err != nil {
		t.Fatal(err)
	}

	dumpStr, err := proto.ReadString(dump)
	if err != nil {
		t.Fatal(err)
	}

	if len(dumpStr) == 0 {
		t.Fatal("expected non-empty dump")
	}

	// Flush all functions
	mustDo(t, c,
		"FUNCTION", "FLUSH",
		proto.String("OK"),
	)

	// Restore from dump
	mustDo(t, c,
		"FUNCTION", "RESTORE", dumpStr,
		proto.String("OK"),
	)

	// Verify function works
	mustDo(t, c,
		"FCALL", "myfunc", "0",
		proto.String("hello"),
	)
}

// Test FUNCTION KILL and FUNCTION STATS
func TestFunctionKillStats(t *testing.T) {
	_, c := runWithClient(t)

	// FUNCTION STATS should return some stats
	resp, err := c.Do("FUNCTION", "STATS")
	if err != nil {
		t.Fatal(err)
	}

	// Parse the response
	result, err := proto.ReadArray(resp)
	if err != nil {
		t.Fatal(err)
	}

	// Verify the response has the running_script field
	if len(result) < 2 {
		t.Fatal("expected at least 2 elements in FUNCTION STATS response")
	}

	firstField, err := proto.ReadString(result[0])
	if err != nil {
		t.Fatal(err)
	}

	if firstField != "running_script" {
		t.Fatalf("expected first field to be 'running_script', got %s", firstField)
	}

	// Test FUNCTION KILL (should report that no function is running)
	mustDo(t, c,
		"FUNCTION", "KILL",
		proto.Error("ERR No function is running"),
	)
}
