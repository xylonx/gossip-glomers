.PHONY: maelstrom-echo

MAELSTROM=${HOME}/repo/maelstrom/maelstrom/maelstrom

./target/debug/echo: ./src/bin/echo.rs
	cargo build

maelstrom-echo: ./target/debug/echo
	$(MAELSTROM) test -w echo --bin ./target/debug/echo --time-limit 5
