# 如何存储FileRecords？
* A segment of the log. Each segment has two components: a log and an index. The log is a FileRecords containing
* the actual messages. The index is an OffsetIndex that maps from logical offsets to physical file positions. Each
* segment has a base offset which is an offset <= the least offset of any message in this segment and > any offset in
* any previous segment.

