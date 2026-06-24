.PHONY: maelstrom-echo maelstrom-uniq-id

MAELSTROM=${HOME}/repo/maelstrom/maelstrom/maelstrom

echo-bin: ./src/bin/echo.rs
	cargo build --bin echo

maelstrom-echo: echo-bin
	$(MAELSTROM) test -w echo --bin ./target/debug/echo --time-limit 5

uniq-ids-bin: ./src/bin/uniq-ids.rs
	cargo build --bin uniq-ids

maelstrom-uniq-id: uniq-ids-bin
	$(MAELSTROM) test -w unique-ids --bin ./target/debug/uniq-ids --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
