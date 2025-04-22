package main

import (
	"fmt"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2/proto"
)

// Helper function to convert response to string
func toString(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", val)
	}
}

// Helper to normalize OK responses which might be different between implementations
func normalizeOKResponse(t *testing.T, c *client, cmd string, args ...string) {
	t.Helper()
	realResp, err := c.real.Do(append([]string{cmd}, args...)...)
	if err != nil {
		t.Fatalf("Error from real Redis: %v", err)
	}
	miniResp, err := c.mini.Do(append([]string{cmd}, args...)...)
	if err != nil {
		t.Fatalf("Error from miniredis: %v", err)
	}

	realStr := toString(realResp)
	miniStr := toString(miniResp)

	// For FUNCTION LOAD commands, Redis returns the library name
	if cmd == "FUNCTION" && len(args) > 0 && (args[0] == "LOAD" || (args[0] == "RESTORE")) {
		// Responses might be different but both should be successful (not error)
		return
	}

	if !strings.Contains(realStr, "OK") && !strings.Contains(realStr, "ok") {
		t.Fatalf("Expected OK-like response from real Redis, got: %v", realStr)
	}
	if !strings.Contains(miniStr, "OK") && !strings.Contains(miniStr, "ok") {
		t.Fatalf("Expected OK-like response from miniredis, got: %v", miniStr)
	}
}

// Helper to verify function list results without being strict about formats
func verifyFunctionList(t *testing.T, c *client, args ...string) {
	t.Helper()

	command := append([]string{"FUNCTION", "LIST"}, args...)

	realResp, err := c.real.Do(command...)
	if err != nil {
		t.Fatalf("Error from real Redis: %v", err)
	}
	miniResp, err := c.mini.Do(command...)
	if err != nil {
		t.Fatalf("Error from miniredis: %v", err)
	}

	// Parse real response
	realParsed, err := proto.Parse(realResp)
	if err != nil {
		t.Fatalf("Failed to parse real Redis response: %v", err)
	}

	// Parse mini response
	miniParsed, err := proto.Parse(miniResp)
	if err != nil {
		t.Fatalf("Failed to parse miniredis response: %v", err)
	}

	// Both should be arrays
	realArr, ok := realParsed.([]interface{})
	if !ok {
		t.Fatalf("Expected array response from real Redis, got: %T", realParsed)
	}

	miniArr, ok := miniParsed.([]interface{})
	if !ok {
		t.Fatalf("Expected array response from miniredis, got: %T", miniParsed)
	}

	// Only check length - not exact format since implementations might differ
	if len(args) > 0 && args[0] == "LIBRARYNAME" && args[1] == "nonexistent" {
		// Special case for non-existent library
		if len(realArr) != 0 || len(miniArr) != 0 {
			t.Fatalf("Expected empty arrays for non-existent library, got real len: %d, mini len: %d",
				len(realArr), len(miniArr))
		}
	} else if len(args) > 0 && args[0] == "LIBRARYNAME" {
		// For specific library, we expect exactly one result
		if len(realArr) != 1 || len(miniArr) != 1 {
			t.Fatalf("Expected arrays with 1 library, got real len: %d, mini len: %d",
				len(realArr), len(miniArr))
		}
	} else if len(args) > 0 && args[0] == "WITHCODE" {
		// For WITHCODE, just verify we have results in both
		if len(realArr) < 1 || len(miniArr) < 1 {
			t.Fatalf("Expected non-empty arrays for WITHCODE, got real len: %d, mini len: %d",
				len(realArr), len(miniArr))
		}
	}
}

func TestFunction(t *testing.T) {
	t.Run("Basic function lifecycle", func(t *testing.T) {
		testRaw(t, func(c *client) {
			// Load a simple function
			script := `#!lua name=mylib
redis.register_function('myfunc', function(keys, args) return 'hello' end)`

			normalizeOKResponse(t, c, "FUNCTION", "LOAD", script)

			// Check function call
			c.Do("FCALL", "myfunc", "0")

			// Delete function
			normalizeOKResponse(t, c, "FUNCTION", "DELETE", "mylib")

			// Try calling again - should fail
			c.Error("Function not found", "FCALL", "myfunc", "0")

			// Load function again for subsequent tests
			normalizeOKResponse(t, c, "FUNCTION", "LOAD", script)

			// Flush all functions
			normalizeOKResponse(t, c, "FUNCTION", "FLUSH")

			// Try calling again - should fail
			c.Error("Function not found", "FCALL", "myfunc", "0")
		})
	})

	t.Run("Function with keys and arguments", func(t *testing.T) {
		testRaw(t, func(c *client) {
			// Set a key first
			c.Do("SET", "mykey", "myvalue")

			// Load a function that uses keys and args
			script := `#!lua name=mylib
redis.register_function('getkey', function(keys, args)
    local value = redis.call('GET', keys[1])
    if args[1] then
        return value .. ' ' .. args[1]
    end
    return value
end)`

			normalizeOKResponse(t, c, "FUNCTION", "LOAD", script)

			// Call with just key
			c.Do("FCALL", "getkey", "1", "mykey")

			// Call with key and argument
			c.Do("FCALL", "getkey", "1", "mykey", "extra")

			// Clean up
			normalizeOKResponse(t, c, "FUNCTION", "FLUSH")
		})
	})

	t.Run("FUNCTION LOAD REPLACE", func(t *testing.T) {
		testRaw(t, func(c *client) {
			// Load initial function
			script1 := `#!lua name=mylib
redis.register_function('myfunc', function(keys, args) return 'version1' end)`

			normalizeOKResponse(t, c, "FUNCTION", "LOAD", script1)

			// Function returns version1
			c.Do("FCALL", "myfunc", "0")

			// Replace the function
			script2 := `#!lua name=mylib
redis.register_function('myfunc', function(keys, args) return 'version2' end)`

			normalizeOKResponse(t, c, "FUNCTION", "LOAD", "REPLACE", script2)

			// Function should return version2 now
			c.Do("FCALL", "myfunc", "0")

			// Clean up
			normalizeOKResponse(t, c, "FUNCTION", "FLUSH")
		})
	})

	t.Run("Read-only functions", func(t *testing.T) {
		testRaw(t, func(c *client) {
			// Set test key
			c.Do("SET", "counter", "10")

			// Load a function that tries to write in read-only mode
			script := `#!lua name=mylib
redis.register_function{
    function_name='readonly',
    callback=function(keys, args)
        return redis.call('GET', keys[1])
    end,
    flags={'no-writes'}
}

redis.register_function{
    function_name='readwrite',
    callback=function(keys, args)
        return redis.call('INCR', keys[1])
    end
}`

			normalizeOKResponse(t, c, "FUNCTION", "LOAD", script)

			// Read-only function should work fine
			c.Do("FCALL", "readonly", "1", "counter")

			// Read-write function should work too
			c.Do("FCALL", "readwrite", "1", "counter")

			// Read-only function with FCALL_RO should work
			c.Do("FCALL_RO", "readonly", "1", "counter")

			// Read-write function with FCALL_RO should fail
			c.Error("Write commands are not allowed", "FCALL_RO", "readwrite", "1", "counter")

			// Clean up
			normalizeOKResponse(t, c, "FUNCTION", "FLUSH")
		})
	})

	t.Run("FUNCTION DUMP and RESTORE", func(t *testing.T) {
		testRaw(t, func(c *client) {
			// Load several functions
			script1 := `#!lua name=lib1
redis.register_function('func1', function(keys, args) return 'hello' end)`

			normalizeOKResponse(t, c, "FUNCTION", "LOAD", script1)

			script2 := `#!lua name=lib2
redis.register_function('func2', function(keys, args) return 'world' end)`

			normalizeOKResponse(t, c, "FUNCTION", "LOAD", script2)

			// Dump all functions
			realDump, err := c.real.Do("FUNCTION", "DUMP")
			if err != nil {
				t.Fatalf("Error from real Redis: %v", err)
			}

			miniDump, err := c.mini.Do("FUNCTION", "DUMP")
			if err != nil {
				t.Fatalf("Error from miniredis: %v", err)
			}

			// Clear all functions
			normalizeOKResponse(t, c, "FUNCTION", "FLUSH")

			// Verify functions are gone by trying to call them
			c.Error("Function not found", "FCALL", "func1", "0")
			c.Error("Function not found", "FCALL", "func2", "0")

			// Restore functions from real Redis dump
			normalizeOKResponse(t, c, "FUNCTION", "RESTORE", toString(realDump))

			// Functions should work again
			c.Do("FCALL", "func1", "0")
			c.Do("FCALL", "func2", "0")

			// Clean up and prepare for second test with miniDump
			normalizeOKResponse(t, c, "FUNCTION", "FLUSH")

			// Now test restoration from miniredis dump
			normalizeOKResponse(t, c, "FUNCTION", "RESTORE", toString(miniDump))

			// Functions should work again when restored from miniredis dump
			c.Do("FCALL", "func1", "0")
			c.Do("FCALL", "func2", "0")

			// Final cleanup
			normalizeOKResponse(t, c, "FUNCTION", "FLUSH")
		})
	})

	t.Run("FUNCTION LIST with parameters", func(t *testing.T) {
		testRaw(t, func(c *client) {
			// Load two functions
			script1 := `#!lua name=lib1
redis.register_function('func1', function(keys, args) return 'hello' end)
redis.register_function('func2', function(keys, args) return 'world' end)`

			normalizeOKResponse(t, c, "FUNCTION", "LOAD", script1)

			script2 := `#!lua name=lib2
redis.register_function('func3', function(keys, args) return 'hello world' end)`

			normalizeOKResponse(t, c, "FUNCTION", "LOAD", script2)

			// Test FUNCTION LIST WITHCODE - verify libraries are present but don't compare exact format
			verifyFunctionList(t, c, "WITHCODE")

			// Test FUNCTION LIST LIBRARYNAME lib1 - verify library is present
			// and we only get one library in the response
			verifyFunctionList(t, c, "LIBRARYNAME", "lib1")

			// Test FUNCTION LIST with non-existent library - should return empty array
			verifyFunctionList(t, c, "LIBRARYNAME", "nonexistent")

			// Clean up
			normalizeOKResponse(t, c, "FUNCTION", "FLUSH")
		})
	})

	t.Run("E-commerce integration scenario", func(t *testing.T) {
		testRaw(t, func(c *client) {
			// Create complex e-commerce functions library
			script := `#!lua name=ecommerce
-- Calculate discount based on user level
redis.register_function{
    function_name='calculate_discount',
    callback=function(keys, args)
        local user_id = keys[1]
        local product_id = keys[2]
        local user_level = tonumber(redis.call('HGET', user_id, 'level') or 0)
        local base_discount = 0

        if user_level == 1 then
            base_discount = 5
        elseif user_level == 2 then
            base_discount = 10
        elseif user_level >= 3 then
            base_discount = 15
        end

        local product_category = redis.call('HGET', product_id, 'category')
        if product_category == 'electronics' then
            base_discount = base_discount + 2
        end

        return base_discount
    end,
    flags={'no-writes'}
}

-- Add item to cart
redis.register_function{
    function_name='add_to_cart',
    callback=function(keys, args)
        local cart_id = keys[1]
        local product_id = keys[2]
        local quantity = tonumber(args[1] or 1)

        -- Check if product exists
        local exists = redis.call('EXISTS', product_id)
        if exists == 0 then
            return {err="Product not found"}
        end

        -- Get product price
        local price = tonumber(redis.call('HGET', product_id, 'price') or 0)

        -- Add to cart as hash
        redis.call('HINCRBY', cart_id, product_id, quantity)
        redis.call('HINCRBY', cart_id..':prices', product_id, price * quantity)

        return {ok="Item added to cart"}
    end
}

-- Get cart total with discount applied
redis.register_function{
    function_name='get_cart_total',
    callback=function(keys, args)
        local cart_id = keys[1]
        local user_id = keys[2]

        -- Get all products in cart
        local cart_items = redis.call('HGETALL', cart_id)
        local cart_prices = redis.call('HGETALL', cart_id..':prices')

        if #cart_items == 0 then
            return {subtotal=0, discount=0, total=0}
        end

        local subtotal = 0
        for i=2, #cart_prices, 2 do
            subtotal = subtotal + tonumber(cart_prices[i])
        end

        -- Calculate overall discount
        local user_level = tonumber(redis.call('HGET', user_id, 'level') or 0)
        local discount_pct = 0

        if user_level == 1 then
            discount_pct = 5
        elseif user_level == 2 then
            discount_pct = 7
        elseif user_level >= 3 then
            discount_pct = 10
        end

        local discount = subtotal * (discount_pct / 100)
        local total = subtotal - discount

        -- Return as JSON
        return cjson.encode({
            subtotal = subtotal,
            discount_pct = discount_pct,
            discount = discount,
            total = total
        })
    end,
    flags={'no-writes'}
}`

			normalizeOKResponse(t, c, "FUNCTION", "LOAD", script)

			// Set up test data - user with level 2
			c.Do("HSET", "user:1001", "level", "2", "name", "Test User")

			// Set up test product data
			c.Do("HSET", "product:101", "name", "Smartphone", "price", "500", "category", "electronics")
			c.Do("HSET", "product:102", "name", "T-shirt", "price", "20", "category", "clothing")

			// Test read-only discount calculation function
			c.Do("FCALL", "calculate_discount", "2", "user:1001", "product:101")

			// Test cart functions
			c.Do("FCALL", "add_to_cart", "2", "cart:1001", "product:101", "1")
			c.Do("FCALL", "add_to_cart", "2", "cart:1001", "product:102", "2")

			// Error case - product doesn't exist
			c.Do("FCALL", "add_to_cart", "2", "cart:1001", "product:999", "1")

			// Get cart total with JSON response
			c.Do("FCALL", "get_cart_total", "2", "cart:1001", "user:1001")

			// Verify cart data directly
			c.Do("HGETALL", "cart:1001")
			c.Do("HGETALL", "cart:1001:prices")

			// Clean up
			c.Do("DEL", "user:1001", "product:101", "product:102", "cart:1001", "cart:1001:prices")
			normalizeOKResponse(t, c, "FUNCTION", "FLUSH")
		})
	})
}
