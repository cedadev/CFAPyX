===============
Release History
===============

2026 (DRAFT)
------------
Accompanies the 2026 first release of ``ArrayPartition`` (DRAFT)

- Revised handling of ``scale_factor`` and ``add_offset`` properties. These are now handled by individual fragments,
but the scaling/offsetting can still be deactivated via the xarray dataset instantiation, this now filters all the way down to the partition-level.
- Fixed bugs relating to single-indexes. Single-valued slices like ``slice(0,1)`` and single-indexes like ``0`` are now distinctly handled internally,
by adjusting the ``extent`` property of partitions. The extent on an indexed dimension remains at size ``1`` but the ``shape`` parameter generates without that dimension.
This is handled internally by skipping the lost dimension in all cases except for when data is accessed, in which case the entire extent is applied, including the single-index value that is skipped in other operations.
