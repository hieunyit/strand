# G2-12 MySQL — số chốt trước lượt chính thức (2026-11-14 22:37:14)
- low/high = 88 / 352 txn/s (calibration.json); spike 2x high 10 s mỗi 3 phút; chu kỳ 10 phút
- soft/hard IOPS = 400 / 600; seed 20261113; fixture bench.big 67.000.000 row, app 100.000 row
- client sysbench-patched sha256 8c49cf79448cbfb3d24846096edf5e6a30fdd38be169b85f4a1ef4667cd5a6f2
