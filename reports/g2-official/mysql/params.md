# G2-12 MySQL — số chốt trước lượt chính thức (2026-11-15 00:53:38)
- low/high = 82 / 328 txn/s (calibration.json); spike 2x high 10 s mỗi 3 phút; chu kỳ 10 phút
- soft/hard IOPS = 400 / 600; seed 20261113; fixture bench.big 20.000.000 row (Amendment 2026-11-14 (2)), app 100.000 row
- client sysbench-patched sha256 8c49cf79448cbfb3d24846096edf5e6a30fdd38be169b85f4a1ef4667cd5a6f2
