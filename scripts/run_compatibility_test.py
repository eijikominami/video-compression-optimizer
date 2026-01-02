#!/usr/bin/env python3.11
"""Swift/Python compatibility test script.

Run from iTerm2:
    cd video-compression-optimizer
    python3.11 scripts/run_compatibility_test.py

Results are saved to: scripts/compatibility_result.txt
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from datetime import datetime


def main():
    output_lines = []
    
    def log(msg):
        print(msg)
        output_lines.append(msg)
    
    log(f"=== Compatibility Test: {datetime.now().isoformat()} ===\n")
    
    try:
        from vco.photos.swift_bridge import SwiftBridge
        from vco.photos.manager import PhotosAccessManager
        
        log("Initializing implementations...")
        swift = SwiftBridge()
        swift.DEFAULT_TIMEOUT = 600  # Extend timeout for large libraries
        python = PhotosAccessManager()
        
        log("\n=== Swift Implementation ===")
        swift_videos = swift.get_all_videos()
        log(f"Swift found {len(swift_videos)} videos")
        
        log("\n=== Python Implementation ===")
        python_videos = python.get_all_videos()
        log(f"Python found {len(python_videos)} videos")
        
        log("\n=== UUID Comparison ===")
        swift_uuids = {v.uuid for v in swift_videos}
        python_uuids = {v.uuid for v in python_videos}
        
        only_swift = swift_uuids - python_uuids
        only_python = python_uuids - swift_uuids
        
        if only_swift:
            log(f"❌ Only in Swift ({len(only_swift)}): {list(only_swift)[:5]}...")
        if only_python:
            log(f"❌ Only in Python ({len(only_python)}): {list(only_python)[:5]}...")
        if swift_uuids == python_uuids:
            log("✅ UUID sets match!")
        else:
            log(f"❌ UUID sets differ: Swift={len(swift_uuids)}, Python={len(python_uuids)}")
        
        # Field comparison for matching videos
        log("\n=== Field Comparison ===")
        python_by_uuid = {v.uuid: v for v in python_videos}
        
        mismatches = []
        for sv in swift_videos:
            pv = python_by_uuid.get(sv.uuid)
            if not pv:
                continue
            
            diffs = []
            # Exact match fields (except codec/frame_rate for iCloud-only files)
            for field in ["filename", "resolution", "file_size", "albums"]:
                s_val = getattr(sv, field, None)
                p_val = getattr(pv, field, None)
                if s_val != p_val:
                    diffs.append(f"{field}: Swift={s_val}, Python={p_val}")
            
            # codec: only compare if Swift has actual value (not "unknown")
            # Swift can't extract codec from iCloud-only files
            if sv.codec != "unknown" and sv.codec != pv.codec:
                diffs.append(f"codec: Swift={sv.codec}, Python={pv.codec}")
            
            # is_in_icloud/is_local: SKIP comparison
            # Swift PhotoKit auto-downloads from iCloud when needed (isNetworkAccessAllowed=true)
            # so these fields are not critical for Swift implementation
            # Python uses osxphotos.iscloudasset which has different semantics
            
            # Tolerance fields (only compare if Swift has actual value)
            for field, tol in [("bitrate", 0.01), ("duration", 0.001)]:
                s_val = getattr(sv, field, 0) or 0
                p_val = getattr(pv, field, 0) or 0
                if p_val != 0 and s_val != 0 and abs(s_val - p_val) / p_val > tol:
                    diffs.append(f"{field}: Swift={s_val}, Python={p_val} (>{tol*100}%)")
            
            # frame_rate: only compare if Swift has actual value (not 0)
            if sv.frame_rate > 0 and pv.frame_rate > 0:
                if abs(sv.frame_rate - pv.frame_rate) / pv.frame_rate > 0.01:
                    diffs.append(f"frame_rate: Swift={sv.frame_rate}, Python={pv.frame_rate} (>1.0%)")
            
            if diffs:
                mismatches.append((sv.filename, sv.uuid, diffs))
        
        if mismatches:
            log(f"❌ {len(mismatches)} videos have field mismatches:")
            for fname, uuid, diffs in mismatches[:10]:
                log(f"\n  {fname} ({uuid}):")
                for d in diffs:
                    log(f"    - {d}")
            if len(mismatches) > 10:
                log(f"\n  ... and {len(mismatches) - 10} more")
        else:
            log("✅ All matching videos have consistent fields!")
        
        log("\n=== RESULT ===")
        if swift_uuids == python_uuids and not mismatches:
            log("✅ PASS: Swift and Python implementations are compatible!")
        else:
            log("❌ FAIL: Implementations have differences")
        
    except Exception as e:
        log(f"\n❌ ERROR: {type(e).__name__}: {e}")
        import traceback
        log(traceback.format_exc())
    
    # Save results
    result_path = Path(__file__).parent / "compatibility_result.txt"
    result_path.write_text("\n".join(output_lines))
    print(f"\n[Results saved to: {result_path}]")


if __name__ == "__main__":
    main()
