#!/usr/bin/env sh

set -x #echo on

BOR_DIR=./running
DATA_DIR=$BOR_DIR/data

./build/bin/bor server -datadir $DATA_DIR \
  -port 30303 \
  -http -http.addr '127.0.0.1' \
  -http.vhosts '*' \
  -http.corsdomain '*' \
  -http.port 8545 \
  -ipcpath $DATA_DIR/bor.ipc \
  -http.api 'eth,net,web3,txpool,bor' \
  -syncmode 'full' \
  -miner.gasprice '30000000000' \
  -miner.gaslimit '30000000' \
  -txpool.accountslots 16 \
  -txpool.globalslots 32768 \
  -txpool.accountqueue 16 \
  -txpool.globalqueue 32768 \
  -txpool.pricelimit '30000000000' \
  -txpool.lifetime '1h30m0s' \
  -gpo.ignoreprice '30000000000' \
  -maxpeers 400 \
  -metrics \
  -bootnodes 'enode://b8f1cc9c5d4403703fbf377116469667d2b1823c0daf16b7250aa576bacf399e42c3930ccfcb02c5df6879565a2b8931335565f0e8d3f8e72385ecf4a4bf160a@3.36.224.80:30303,enode://8729e0c825f3d9cad382555f3e46dcff21af323e89025a0e6312df541f4a9e73abfa562d64906f5e59c51fe6f0501b3e61b07979606c56329c020ed739910759@54.194.245.5:30303'
  # -bootnodes 'enode://0cb82b395094ee4a2915e9714894627de9ed8498fb881cec6db7c65e8b9a5bd7f2f25cc84e71e89d0947e51c76e85d0847de848c7782b13c0255247a6758178c@44.232.55.71:30303,enode://88116f4295f5a31538ae409e4d44ad40d22e44ee9342869e7d68bdec55b0f83c1530355ce8b41fbec0928a7d75a5745d528450d30aec92066ab6ba1ee351d710@159.203.9.164:30303,enode://3178257cd1e1ab8f95eeb7cc45e28b6047a0432b2f9412cff1db9bb31426eac30edeb81fedc30b7cd3059f0902b5350f75d1b376d2c632e1b375af0553813e6f@35.221.13.28:30303,enode://16d9a28eadbd247a09ff53b7b1f22231f6deaf10b86d4b23924023aea49bfdd51465b36d79d29be46a5497a96151a1a1ea448f8a8666266284e004306b2afb6e@35.199.4.13:30303,enode://ef271e1c28382daa6ac2d1006dd1924356cfd843dbe88a7397d53396e0741ca1a8da0a113913dee52d9071f0ad8d39e3ce87aa81ebc190776432ee7ddc9d9470@35.230.116.151:30303'
  # -pprof -pprof.port 7071 -pprof.addr '0.0.0.0' \
  # -nodiscover true \
  # -txpool.nolocals \