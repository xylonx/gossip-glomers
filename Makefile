.PHONY: maelstrom-echo maelstrom-uniq-id maelstrom-broadcast-single-node

MAELSTROM=${HOME}/repo/maelstrom/maelstrom/maelstrom

echo-bin: ./src/bin/echo.rs
	cargo build --bin echo

maelstrom-echo: echo-bin
	$(MAELSTROM) test -w echo --bin ./target/debug/echo --time-limit 5

uniq-ids-bin: ./src/bin/uniq-ids.rs
	cargo build --bin uniq-ids

maelstrom-uniq-id: uniq-ids-bin
	$(MAELSTROM) test -w unique-ids --bin ./target/debug/uniq-ids --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition

broadcast-bin: ./src/bin/broadcast.rs
	cargo build --bin broadcast

maelstrom-broadcast-single-node: broadcast-bin
	$(MAELSTROM) test -w broadcast --bin ./target/debug/broadcast --node-count 1 --time-limit 20 --rate 10

