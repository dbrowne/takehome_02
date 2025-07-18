# Code Coverage Makefile
#

.PHONY: coverage coverage-html coverage-lcov clean-coverage

# Run tests with coverage and open HTML report
coverage:
	cargo llvm-cov --html
	@echo "Opening coverage report..."
	@open target/llvm-cov/html/index.html || xdg-open target/llvm-cov/html/index.html

# Generate HTML report
coverage-html:
	cargo llvm-cov --html

# Generate lcov report for CI
coverage-lcov:
	cargo llvm-cov --lcov --output-path lcov.info

# Clean coverage data
clean-coverage:
	cargo llvm-cov clean
