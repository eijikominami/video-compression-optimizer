"""CLI commands for Video Compression Optimizer.

Usage:
    vco scan [--from YYYY-MM] [--to YYYY-MM] [--json]
    vco convert [--quality high|balanced|compression] [--dry-run]
    vco import --list
    vco import <review_id>
    vco import --all
    vco config [--json]
    vco config set <key> <value>
"""

import json
import sys
from datetime import datetime, timezone
from typing import Any

import click
from rich.console import Console
from rich.table import Table

from vco.analyzer.analyzer import CompressionAnalyzer
from vco.cli.i18n import get_help
from vco.config.manager import ConfigManager
from vco.photos.manager import PhotosAccessManager
from vco.services.scan import ScanService

console = Console()


def utc_to_local(utc_dt: datetime) -> datetime:
    """Convert UTC datetime to local timezone.

    Args:
        utc_dt: UTC datetime (naive or aware)

    Returns:
        Local timezone datetime
    """
    # If naive datetime, assume it's UTC
    if utc_dt.tzinfo is None:
        utc_dt = utc_dt.replace(tzinfo=timezone.utc)
    return utc_dt.astimezone()


def format_size(size_bytes: int | float) -> str:
    """Format bytes as human-readable size."""
    size_float = float(size_bytes)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(size_float) < 1024.0:
            return f"{size_float:.1f} {unit}"
        size_float /= 1024.0
    return f"{size_float:.1f} PB"


def format_duration(seconds: float) -> str:
    """Format seconds as human-readable duration."""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)

    if hours > 0:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    return f"{minutes}:{secs:02d}"


def parse_date(date_str: str) -> datetime | None:
    """Parse YYYY-MM date string."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y-%m")
    except ValueError:
        raise click.BadParameter(f"Invalid date format: {date_str}. Use YYYY-MM.")


@click.group()
@click.pass_context
def cli(ctx):
    """Video Compression Optimizer - Apple Photos video compression tool"""
    ctx.ensure_object(dict)
    ctx.obj["config"] = ConfigManager()


# Override help text dynamically based on locale
cli.help = get_help("cli.description")


@cli.command()
@click.option("--from", "from_date", type=str, help=get_help("scan.from_date"))
@click.option("--to", "to_date", type=str, help=get_help("scan.to_date"))
@click.option("--top-n", type=int, help=get_help("scan.top_n"))
@click.option("--json", "output_json", is_flag=True, help=get_help("scan.json"))
@click.pass_context
def scan(
    ctx,
    from_date: str | None,
    to_date: str | None,
    top_n: int | None,
    output_json: bool,
):
    """Scan Apple Photos library and display conversion candidates."""
    config = ctx.obj["config"]

    # Parse dates
    from_dt = parse_date(from_date) if from_date else None
    to_dt = parse_date(to_date) if to_date else None

    # Use osxphotos (PhotosAccessManager) for accurate codec detection
    photos_manager = PhotosAccessManager()

    analyzer = CompressionAnalyzer()
    scan_service = ScanService(photos_manager=photos_manager, analyzer=analyzer)

    if not output_json:
        console.print("[bold]Scanning Photos library...[/bold]")

    # Perform scan
    try:
        result = scan_service.scan(
            from_date=from_dt, to_date=to_dt, quality_preset=config.get("conversion.quality_preset")
        )
    except Exception as e:
        if output_json:
            click.echo(json.dumps({"error": str(e)}))
        else:
            console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)

    # Apply top-n selection if specified
    if top_n is not None:
        if top_n <= 0:
            if output_json:
                click.echo(json.dumps({"error": "--top-n must be a positive integer"}))
            else:
                console.print("[red]Error: --top-n must be a positive integer[/red]")
            sys.exit(1)

        result.candidates = scan_service.select_top_n(result.candidates, top_n)
        # Recalculate summary for selected candidates
        top_n_summary = scan_service.calculate_top_n_summary(result.candidates)
        result.summary.conversion_candidates = top_n_summary["count"]
        result.summary.estimated_total_savings_bytes = top_n_summary["estimated_savings"]
        result.summary.estimated_total_savings_percent = top_n_summary["estimated_savings_percent"]

    # Save candidates
    scan_service.save_candidates(result)

    if output_json:
        click.echo(json.dumps(result.to_dict(), indent=2))
        return

    # Display summary
    summary = result.summary
    console.print()
    console.print("[bold]Scan Summary[/bold]")
    console.print(f"  Total videos: {summary.total_videos}")
    console.print(f"  Conversion candidates: {summary.conversion_candidates}")
    console.print(f"  Already optimized: {summary.already_optimized}")
    console.print(f"  Professional format: {summary.professional}")
    console.print(f"  Skipped: {summary.skipped}")
    console.print()

    if not result.candidates:
        console.print("[green]No conversion candidates found.[/green]")
        return

    # Display candidates table
    table = Table(title="Conversion Candidates")
    table.add_column("Filename", style="cyan")
    table.add_column("Codec", style="yellow")
    table.add_column("Resolution")
    table.add_column("Duration")
    table.add_column("Size")
    table.add_column("Location", style="magenta")

    for candidate in result.candidates[:20]:  # Show first 20
        video = candidate.video

        # Determine location status
        location = "Local" if video.is_local else "iCloud"

        table.add_row(
            video.filename[:40] + ("..." if len(video.filename) > 40 else ""),
            video.codec,
            f"{video.resolution[0]}x{video.resolution[1]}",
            format_duration(video.duration),
            format_size(video.file_size),
            location,
        )

    console.print(table)

    if len(result.candidates) > 20:
        console.print(f"[dim]... and {len(result.candidates) - 20} more candidates[/dim]")

    console.print()
    console.print(f"[dim]Candidates saved to: {scan_service.output_dir / 'candidates.json'}[/dim]")


# Override scan help text dynamically based on locale
scan.help = get_help("scan.description")


@cli.command()
@click.option(
    "--quality",
    type=click.Choice(["high", "balanced", "balanced+", "compression"]),
    default="balanced+",
    help=get_help("convert.quality"),
)
@click.option("--top-n", type=int, help=get_help("convert.top_n"))
@click.option("--dry-run", is_flag=True, help=get_help("convert.dry_run"))
@click.option("--skip-icloud", is_flag=True, help=get_help("convert.skip_icloud"))
@click.option("--download-timeout", type=int, help=get_help("convert.download_timeout"))
@click.option(
    "--parallel",
    "-p",
    type=int,
    default=3,
    help="Number of concurrent transfers (1-10, default: 3)",
)
@click.option("--yes", "-y", is_flag=True, help=get_help("convert.yes"))
@click.pass_context
def convert(
    ctx,
    quality: str,
    top_n: int | None,
    dry_run: bool,
    skip_icloud: bool,
    download_timeout: int | None,
    parallel: int,
    yes: bool,
):
    """Convert candidate videos to H.265."""
    config = ctx.obj["config"]

    # Get download timeout from option or config
    timeout = download_timeout or config.get("conversion.download_timeout")

    # Load candidates
    scan_service = ScanService()
    result = scan_service.load_candidates()

    if result is None or not result.candidates:
        console.print("[yellow]No candidates found. Run 'vco scan' first.[/yellow]")
        sys.exit(1)

    # Apply top-n selection if specified
    candidates_to_convert = result.candidates
    if top_n is not None:
        if top_n <= 0:
            console.print("[red]Error: --top-n must be a positive integer[/red]")
            sys.exit(1)
        candidates_to_convert = scan_service.select_top_n(result.candidates, top_n)
        console.print(
            f"[bold]Selected top {len(candidates_to_convert)} candidates by file size[/bold]"
        )

    # Classify candidates: local vs iCloud
    local_candidates = [c for c in candidates_to_convert if c.video.is_local]
    icloud_candidates = [c for c in candidates_to_convert if not c.video.is_local]

    console.print(f"[bold]Found {len(candidates_to_convert)} candidates for conversion[/bold]")
    if icloud_candidates:
        console.print(f"  Local: {len(local_candidates)}")
        console.print(f"  iCloud: {len(icloud_candidates)}")
    console.print(f"Quality preset: {quality}")

    # Handle iCloud candidates
    downloaded_candidates: list = []
    if icloud_candidates and skip_icloud:
        # Skip iCloud videos
        console.print()
        console.print(
            f"[yellow]⚠ Skipping {len(icloud_candidates)} iCloud-only videos (--skip-icloud).[/yellow]"
        )

    # Show unified summary and single confirmation prompt
    if not skip_icloud and icloud_candidates:
        total_icloud_size = sum(c.video.file_size for c in icloud_candidates)
        console.print()
        console.print("[bold]Planned actions:[/bold]")
        console.print(
            f"  1. Download {len(icloud_candidates)} iCloud videos ({format_size(total_icloud_size)})"
        )
        console.print(f"  2. Upload {len(local_candidates) + len(icloud_candidates)} files to S3")
        console.print(f"  3. Process with quality preset: {quality}")
    elif local_candidates:
        console.print()
        console.print("[bold]Planned actions:[/bold]")
        console.print(f"  1. Upload {len(local_candidates)} files to S3")
        console.print(f"  2. Process with quality preset: {quality}")

    console.print()
    console.print("[yellow]Files will be uploaded to S3 and processed asynchronously.[/yellow]")
    console.print("[yellow]Use 'vco status' to check progress.[/yellow]")
    console.print()

    # Single confirmation prompt
    if not yes:
        if not click.confirm("Do you want to proceed?"):
            console.print("Cancelled.")
            return

    # Download iCloud videos (skip_confirm=True since we already confirmed)
    if icloud_candidates and not skip_icloud:
        downloaded_candidates = _download_icloud_videos_parallel(
            icloud_candidates, timeout, parallel, skip_confirm=True
        )

    # Combine local and downloaded candidates
    final_candidates = local_candidates + downloaded_candidates

    if not final_candidates:
        console.print("[yellow]No local files available for conversion.[/yellow]")
        if icloud_candidates and not skip_icloud:
            console.print(
                "[dim]All iCloud downloads failed. Check network connection and try again.[/dim]"
            )
        sys.exit(1)

    if dry_run:
        console.print("[yellow]Dry run mode - no actual conversion will be performed[/yellow]")
        console.print()

        # Show what would be converted
        table = Table(title="Would Convert")
        table.add_column("Filename", style="cyan")
        table.add_column("Size")
        table.add_column("Est. Savings", style="green")

        for candidate in final_candidates:
            video = candidate.video
            table.add_row(
                video.filename[:50],
                format_size(video.file_size),
                format_size(candidate.estimated_savings_bytes),
            )

        console.print(table)
        return

    # Check AWS configuration
    aws_config = config.config.aws
    if not aws_config.s3_bucket or not aws_config.role_arn:
        console.print(
            "[red]AWS configuration not set. Run 'vco config set aws.s3_bucket <bucket>' and 'vco config set aws.role_arn <arn>'[/red]"
        )
        sys.exit(1)

    # Execute async conversion (skip_confirm=True since we already confirmed)
    _convert_async_parallel(ctx, final_candidates, quality, aws_config, parallel, skip_confirm=True)


# Override convert help text dynamically based on locale
convert.help = get_help("convert.description")


@cli.command("import")
@click.option("--list", "list_mode", is_flag=True, help=get_help("import.list"))
@click.option("--all", "all_mode", is_flag=True, help=get_help("import.all"))
@click.option("--clear", "clear_mode", is_flag=True, help=get_help("import.clear"))
@click.option("--remove", "remove_id", help=get_help("import.remove"))
@click.option("--json", "output_json", is_flag=True, help=get_help("import.json"))
@click.option("--yes", "-y", is_flag=True, help=get_help("import.yes"))
@click.option(
    "--delete-original",
    is_flag=True,
    help="Delete original video from Photos after import",
)
@click.option(
    "--force",
    is_flag=True,
    help="Import even if metadata verification fails",
)
@click.argument("item_id", required=False)
@click.pass_context
def import_cmd(
    ctx,
    list_mode: bool,
    all_mode: bool,
    clear_mode: bool,
    remove_id: str | None,
    output_json: bool,
    yes: bool,
    delete_original: bool,
    force: bool,
    item_id: str | None,
):
    """Import converted videos to Photos library."""
    from vco.services.aws_import import AwsImportService
    from vco.services.unified_import import UnifiedImportService

    config = ctx.obj["config"]
    aws_config = config.config.aws

    # Initialize AWS service if configured
    aws_service = None
    api_url = getattr(aws_config, "async_api_url", None)
    if not api_url:
        api_url = f"https://dln48ri1di.execute-api.{aws_config.region}.amazonaws.com/dev"

    if aws_config.s3_bucket:
        try:
            aws_service = AwsImportService(
                api_url=api_url,
                s3_bucket=aws_config.s3_bucket,
                region=aws_config.region,
                profile_name=aws_config.profile or None,
            )
        except Exception:
            # AWS service initialization failed
            pass

    unified_service = UnifiedImportService(
        aws_service=aws_service,
    )

    # --clear mode
    if clear_mode:
        # Get all importable items to show what will be deleted
        list_result = unified_service.list_all_importable()
        aws_items = [item for item in list_result.all_items if item.source == "aws"]

        if not aws_items:
            console.print("[green]No items available for removal.[/green]")
            return

        console.print("[yellow]Will remove the following items and files:[/yellow]")
        console.print(f"  • {len(aws_items)} AWS items (S3 files will be deleted)")

        if not yes and not click.confirm("Do you want to proceed?"):
            console.print("Cancelled.")
            return

        result = unified_service.clear_all_queues()
        if result.success:
            console.print(f"[green]✓ Removed {result.total_items_removed} items total.[/green]")

            if result.aws_items_removed > 0:
                console.print(
                    f"[green]✓ AWS: {result.aws_items_removed} items, {result.aws_files_deleted} S3 files deleted.[/green]"
                )

            if result.total_files_failed > 0:
                console.print(
                    f"[yellow]⚠ {result.total_files_failed} file deletions failed.[/yellow]"
                )
                for error in result.error_details[:3]:  # Show first 3 errors
                    console.print(f"  {error}")
                if len(result.error_details) > 3:
                    console.print(f"  ... and {len(result.error_details) - 3} more errors")
        else:
            console.print("[red]✗ Failed to clear queues.[/red]")
            sys.exit(1)
        return

    # --remove mode
    if remove_id:
        remove_result = unified_service.remove_item(remove_id)
        if remove_result.success:
            source_label = "AWS" if remove_result.source == "aws" else "local"
            console.print(f"[green]✓ Removed {remove_id} ({source_label}).[/green]")

            if remove_result.source == "local":
                file_status = []
                if remove_result.file_deleted:
                    file_status.append("video file")
                if remove_result.metadata_deleted:
                    file_status.append("metadata file")

                if file_status:
                    console.print(f"[green]✓ Deleted {', '.join(file_status)}.[/green]")
            elif remove_result.source == "aws":
                if remove_result.s3_deleted:
                    console.print("[green]✓ Deleted S3 file.[/green]")
                else:
                    console.print("[yellow]⚠ Failed to delete S3 file.[/yellow]")
        else:
            console.print(f"[red]✗ Failed to remove: {remove_result.error_message}[/red]")
            sys.exit(1)
        return

    # --list mode
    if list_mode:
        list_result = unified_service.list_all_importable()

        if output_json:
            items = [
                {
                    "item_id": item.item_id,
                    "source": item.source,
                    "original_filename": item.original_filename,
                    "converted_filename": item.converted_filename,
                    "original_size": item.original_size,
                    "converted_size": item.converted_size,
                    "compression_ratio": item.compression_ratio,
                    "ssim_score": item.ssim_score,
                    "albums": item.albums,
                    "capture_date": item.capture_date.isoformat() if item.capture_date else None,
                    "task_id": item.task_id,
                    "file_id": item.file_id,
                }
                for item in list_result.all_items
            ]
            click.echo(
                json.dumps(
                    {
                        "items": items,
                        "aws_available": list_result.aws_available,
                        "aws_error": list_result.aws_error,
                    },
                    indent=2,
                )
            )
            return

        # Show AWS warning if unavailable
        if not list_result.aws_available:
            console.print(f"[yellow]⚠ AWS unavailable: {list_result.aws_error}[/yellow]")
            console.print("[dim]Showing local items only.[/dim]")
            console.print()

        if list_result.total_count == 0:
            console.print("[green]No pending imports.[/green]")
            return

        console.print(f"[bold]Pending imports: {list_result.total_count}[/bold]")
        if list_result.local_items:
            console.print(f"  Local: {len(list_result.local_items)}")
        if list_result.aws_items:
            console.print(f"  AWS: {len(list_result.aws_items)}")
        console.print()

        table = Table(title="Pending Imports")
        table.add_column("ID", style="cyan")
        table.add_column("Filename")
        table.add_column("Original")
        table.add_column("Converted")
        table.add_column("Ratio", style="green")
        table.add_column("SSIM")
        table.add_column("VMAF")

        for item in list_result.all_items:
            # Calculate savings
            savings_ratio = f"{item.compression_ratio:.1f}x" if item.compression_ratio > 0 else "-"
            ssim_str = f"{item.ssim_score:.4f}" if item.ssim_score > 0 else "-"
            vmaf_str = f"{item.vmaf_score:.1f}" if item.vmaf_score > 0 else "-"

            # Truncate filename
            filename = item.converted_filename
            if len(filename) > 25:
                filename = filename[:22] + "..."

            # ID display (full ID for easy copy-paste)
            display_id = item.item_id

            table.add_row(
                display_id,
                filename,
                format_size(item.original_size),
                format_size(item.converted_size),
                savings_ratio,
                ssim_str,
                vmaf_str,
            )

        console.print(table)
        console.print()
        console.print(
            "[dim]Use 'vco import <id>' for single import, 'vco import --all' for batch import[/dim]"
        )
        return

    # --all mode
    if all_mode:
        list_result = unified_service.list_all_importable()

        # Show AWS warning if unavailable
        if not list_result.aws_available:
            console.print(f"[yellow]⚠ AWS unavailable: {list_result.aws_error}[/yellow]")
            console.print("[dim]Importing local items only.[/dim]")
            console.print()

        if list_result.total_count == 0:
            console.print("[green]No pending imports.[/green]")
            return

        console.print(f"[bold]Importing {list_result.total_count} videos in batch[/bold]")
        if list_result.local_items:
            console.print(f"  Local: {len(list_result.local_items)}")
        if list_result.aws_items:
            console.print(f"  AWS: {len(list_result.aws_items)} (parallel download)")
        console.print()
        if delete_original:
            console.print("[yellow]Note: Original videos will be deleted after import.[/yellow]")
        else:
            console.print(
                "[yellow]Note: You will be prompted to delete original videos after import.[/yellow]"
            )
        console.print()

        if not yes and not click.confirm("Do you want to proceed?"):
            console.print("Cancelled.")
            return

        console.print()

        # Use rich Progress for AWS downloads (same style as single item import)
        from rich.progress import (
            BarColumn,
            DownloadColumn,
            Progress,
            SpinnerColumn,
            TaskID,
            TextColumn,
            TimeRemainingColumn,
            TransferSpeedColumn,
        )

        # Track import status
        download_tasks: dict[str, TaskID] = {}  # filename -> task_id
        import_tasks: dict[str, bool] = {}  # filename -> imported flag

        def download_progress_callback(
            filename: str, percent: int, downloaded: int, total: int
        ) -> None:
            """Display download progress for AWS files using rich Progress."""
            if filename not in download_tasks:
                task_id = progress.add_task(f"Downloading {filename}", total=total)
                download_tasks[filename] = task_id
            progress.update(download_tasks[filename], completed=downloaded)

            # Hide progress bar when download is complete
            if downloaded >= total:
                progress.update(download_tasks[filename], visible=False)

        def status_callback(
            filename: str,
            status: str,
            verification_result: Any = None,
            albums: list[str] | None = None,
        ) -> None:
            """Display status updates for import process."""
            if status == "importing" and filename not in import_tasks:
                import_tasks[filename] = True
                # Display importing status with bullet point
                progress.console.print(
                    f"[bold blue]• Importing {filename} to Photos...[/bold blue]"
                )
            elif status == "imported":
                # Display import completion
                progress.console.print(f"[green]✓ Imported {filename}[/green]")
                # Display verified metadata
                if verification_result and not verification_result.has_mismatch:
                    if verification_result.capture_date.matches:
                        date_val = verification_result.capture_date.original_value
                        if date_val:
                            progress.console.print(f"  Capture Date: {date_val} [green]✓[/green]")
                    if verification_result.gps_location.matches:
                        gps_val = verification_result.gps_location.original_value
                        if gps_val:
                            progress.console.print(f"  GPS: {gps_val} [green]✓[/green]")
                    if albums:
                        albums_str = ", ".join(albums)
                        progress.console.print(f"  Albums: {albums_str} [green]✓[/green]")

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            DownloadColumn(),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            batch_result = unified_service.import_all(
                progress_callback=download_progress_callback,
                status_callback=status_callback,
                delete_original=delete_original,
                force_import=force,
            )

        # Show completion message

        if output_json:
            click.echo(
                json.dumps(
                    {
                        "total": batch_result.total,
                        "successful": batch_result.successful,
                        "failed": batch_result.failed,
                        "local_total": batch_result.local_total,
                        "local_successful": batch_result.local_successful,
                        "aws_total": batch_result.aws_total,
                        "aws_successful": batch_result.aws_successful,
                        "metadata_verified_count": batch_result.metadata_verified_count,
                        "metadata_mismatch_count": batch_result.metadata_mismatch_count,
                        "skipped_files": batch_result.skipped_files,
                        "results": [
                            {
                                "success": r.success,
                                "item_id": r.item_id,
                                "source": r.source,
                                "original_filename": r.original_filename,
                                "converted_filename": r.converted_filename,
                                "albums": r.albums,
                                "error_message": r.error_message,
                                "downloaded": r.downloaded,
                                "s3_deleted": r.s3_deleted,
                                "metadata_verified": r.metadata_verified,
                                "metadata_mismatch": r.metadata_mismatch,
                            }
                            for r in batch_result.results
                        ],
                    },
                    indent=2,
                )
            )
            return

        console.print()
        console.print("[bold]Import Complete[/bold]")
        console.print(f"  Total: {batch_result.total}")
        console.print(f"  Successful: [green]{batch_result.successful}[/green]")
        console.print(f"  Failed: [red]{batch_result.failed}[/red]")
        if batch_result.metadata_verified_count > 0:
            console.print(
                f"  Metadata verified: [green]{batch_result.metadata_verified_count}[/green]"
            )
        if batch_result.metadata_mismatch_count > 0:
            console.print(
                f"  Metadata mismatch: [yellow]{batch_result.metadata_mismatch_count}[/yellow]"
            )

        if batch_result.local_total > 0 or batch_result.aws_total > 0:
            console.print()
            console.print("[dim]Breakdown:[/dim]")
            if batch_result.local_total > 0:
                console.print(
                    f"  Local: {batch_result.local_successful}/{batch_result.local_total} successful"
                )
            if batch_result.aws_total > 0:
                console.print(
                    f"  AWS: {batch_result.aws_successful}/{batch_result.aws_total} successful"
                )

        # Show skipped files due to metadata mismatch with details
        if batch_result.skipped_files:
            console.print()
            console.print("[yellow]Skipped (metadata mismatch):[/yellow]")
            mismatch_results = [
                r for r in batch_result.results if r.metadata_mismatch and r.verification_result
            ]
            for r in mismatch_results[:5]:
                vr = r.verification_result
                console.print(f"\n[red]✗ {r.converted_filename}[/red]")
                console.print("  [bold]Field          Original                    Converted[/bold]")
                console.print("  " + "─" * 55)

                # Capture date
                cd = vr.capture_date
                cd_status = "" if cd.matches else " ← MISMATCH"
                cd_orig = str(cd.original_value)[:25] if cd.original_value else "(none)"
                cd_conv = str(cd.converted_value)[:15] if cd.converted_value else "(missing)"
                console.print(f"  Capture Date   {cd_orig:<25} {cd_conv}{cd_status}")

                # GPS location
                gps = vr.gps_location
                gps_status = "" if gps.matches else " ← MISMATCH"
                gps_orig = str(gps.original_value)[:25] if gps.original_value else "(none)"
                gps_conv = str(gps.converted_value)[:15] if gps.converted_value else "(missing)"
                console.print(f"  GPS Location   {gps_orig:<25} {gps_conv}{gps_status}")

                # Albums
                alb = vr.album_info
                alb_orig = ", ".join(alb.original_value[:2]) if alb.original_value else "(none)"
                if alb.original_value and len(alb.original_value) > 2:
                    alb_orig += f" (+{len(alb.original_value) - 2})"
                console.print(f"  Albums         {alb_orig[:25]:<25} (verified in JSON)")

            if len(mismatch_results) > 5:
                console.print(f"\n  ... and {len(mismatch_results) - 5} more")
            console.print("\n[dim]Use --force to import anyway.[/dim]")

        if batch_result.failed > 0:
            console.print()
            console.print("[red]Errors:[/red]")
            for r in batch_result.results:
                if not r.success and not r.metadata_mismatch:
                    source_label = f"[{r.source}]"
                    console.print(f"  - {source_label} {r.converted_filename}: {r.error_message}")

        if batch_result.successful > 0:
            console.print()
            # Handle original deletion for successful imports
            successful_results = [r for r in batch_result.results if r.success and r.original_uuid]

            if delete_original:
                # --delete-original flag: delete all originals without prompting
                deleted_count = 0
                failed_count = 0
                for r in successful_results:
                    if r.original_uuid:
                        delete_result = unified_service.delete_original_video(
                            r.original_uuid,
                            r.original_filename or "unknown",
                        )
                        if delete_result.success:
                            deleted_count += 1
                        else:
                            failed_count += 1
                            console.print(
                                f"[yellow]Warning: Failed to delete original: {r.original_filename}[/yellow]"
                            )
                if deleted_count > 0:
                    console.print(f"[green]Original videos moved to trash: {deleted_count}[/green]")
                if failed_count > 0:
                    console.print(f"[yellow]Failed to delete: {failed_count}[/yellow]")
            elif successful_results:
                # No --delete-original: prompt for deletion (regardless of -y flag)
                if click.confirm(
                    f"Delete {len(successful_results)} original video(s)?", default=False
                ):
                    deleted_count = 0
                    failed_count = 0
                    for r in successful_results:
                        if r.original_uuid:
                            delete_result = unified_service.delete_original_video(
                                r.original_uuid,
                                r.original_filename or "unknown",
                            )
                            if delete_result.success:
                                deleted_count += 1
                            else:
                                failed_count += 1
                    if deleted_count > 0:
                        console.print(
                            f"[green]Original videos moved to trash: {deleted_count}[/green]"
                        )
                    if failed_count > 0:
                        console.print(f"[yellow]Failed to delete: {failed_count}[/yellow]")
                else:
                    console.print(
                        "[yellow]Note: Original videos remain in Photos library.[/yellow]"
                    )

        return

    # Single import mode
    if not item_id:
        console.print("[red]Error: Specify item_id or use --list or --all option.[/red]")
        console.print()
        console.print("Usage:")
        console.print("  vco import --list        List pending imports")
        console.print("  vco import <item_id>     Import specified video")
        console.print("  vco import --all         Import all videos")
        console.print()
        console.print("Item ID formats:")
        console.print("  Local: abc123            (review ID)")
        console.print("  AWS:   task-id:file-id   (task:file format)")
        sys.exit(1)

    # Determine if AWS or local item
    is_aws = ":" in item_id

    if is_aws:
        # AWS item - import directly without preview
        console.print(f"[bold]Import AWS item: {item_id}[/bold]")
        console.print()
        console.print("Actions:")
        console.print("  1. Download from S3")
        console.print("  2. Import to Photos")
        console.print("  3. Delete S3 file")
        if delete_original:
            console.print("  4. Delete original video from Photos")
        console.print()
        if not delete_original:
            console.print(
                "[yellow]Note: After import, manually delete original video in Photos app.[/yellow]"
            )
            console.print()

        if not yes and not click.confirm("Do you want to proceed?"):
            console.print("Cancelled.")
            return

        # Import with progress bar for download and spinner for Photos import
        from rich.progress import (
            BarColumn,
            DownloadColumn,
            Progress,
            SpinnerColumn,
            TextColumn,
            TimeRemainingColumn,
            TransferSpeedColumn,
        )

        # Get original UUID from metadata if delete_original is requested
        original_uuid = None
        should_delete_original = delete_original

        # Track current filename for status callback
        current_filename = None

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            DownloadColumn(),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            download_task = None

            def progress_callback(filename: str, percentage: int, downloaded: int, total: int):
                nonlocal download_task, current_filename
                current_filename = filename
                if download_task is None:
                    download_task = progress.add_task(f"Downloading {filename}", total=total)
                progress.update(download_task, completed=downloaded)

            def status_callback(
                filename: str,
                status: str,
                verification_result: Any = None,
                albums: list[str] | None = None,
            ) -> None:
                """Display status updates for import process."""
                nonlocal download_task
                if status == "verifying":
                    # Hide download task and show verify text
                    if download_task is not None:
                        progress.update(download_task, visible=False)
                    # Simple text output for verifying status
                    progress.console.print(
                        f"[bold blue]Verifying metadata for {filename}...[/bold blue]"
                    )
                elif status == "importing":
                    # Simple text output for importing status (no progress bar)
                    if download_task is not None:
                        progress.update(download_task, visible=False)
                    progress.console.print(
                        f"[bold blue]Importing {filename} to Photos...[/bold blue]"
                    )
                elif status == "imported":
                    # Display import completion with verified metadata
                    progress.console.print(f"[green]✓ Imported {filename}[/green]")
                    if verification_result and not verification_result.has_mismatch:
                        if verification_result.capture_date.matches:
                            date_val = verification_result.capture_date.original_value
                            if date_val:
                                progress.console.print(
                                    f"  Capture Date: {date_val} [green]✓[/green]"
                                )
                        if verification_result.gps_location.matches:
                            gps_val = verification_result.gps_location.original_value
                            if gps_val:
                                progress.console.print(f"  GPS: {gps_val} [green]✓[/green]")
                        if albums:
                            albums_str = ", ".join(albums)
                            progress.console.print(f"  Albums: {albums_str} [green]✓[/green]")

            import_result = unified_service.import_item(
                item_id,
                progress_callback=progress_callback,
                status_callback=status_callback,
                delete_original=should_delete_original,
                original_uuid=original_uuid,
                force_import=force,
            )

        # Handle original deletion prompt if import succeeded and --delete-original not specified
        if import_result.success and not delete_original:
            # Prompt for original deletion (regardless of -y flag)
            if click.confirm("Delete original video?", default=False):
                # User responded "y" - attempt deletion
                # Get original UUID from import result
                if import_result.original_uuid:
                    delete_result = unified_service.delete_original_video(
                        import_result.original_uuid,
                        import_result.original_filename or "unknown",
                    )
                    if delete_result.success:
                        console.print(
                            f"[green]Original video moved to trash: {import_result.original_filename}[/green]"
                        )
                    else:
                        console.print(
                            f"[yellow]Warning: Failed to delete original: {delete_result.error_message}[/yellow]"
                        )
                else:
                    console.print(
                        "[yellow]Warning: Cannot delete original - UUID not available[/yellow]"
                    )
            else:
                # User responded "n" - show reminder
                console.print("[yellow]Note: Original video remains in Photos library.[/yellow]")
    else:
        # Non-AWS item ID format - not supported
        console.print(f"[red]Error: Invalid item ID format: {item_id}[/red]")
        console.print()
        console.print("Item ID must be in AWS format: task_id:file_id")
        console.print("Use 'vco import --list' to see available items.")
        sys.exit(1)

    if output_json:
        click.echo(
            json.dumps(
                {
                    "success": import_result.success,
                    "item_id": import_result.item_id,
                    "source": import_result.source,
                    "original_filename": import_result.original_filename,
                    "converted_filename": import_result.converted_filename,
                    "albums": import_result.albums,
                    "error_message": import_result.error_message,
                    "downloaded": import_result.downloaded,
                    "s3_deleted": import_result.s3_deleted,
                    "metadata_verified": import_result.metadata_verified,
                    "metadata_mismatch": import_result.metadata_mismatch,
                    "verification_result": import_result.verification_result.to_dict()
                    if import_result.verification_result
                    else None,
                },
                indent=2,
            )
        )
        return

    if import_result.success:
        console.print("[green]✓ Import to Photos completed[/green]")
        if import_result.source == "aws":
            if import_result.downloaded:
                console.print("[green]✓ Downloaded from S3[/green]")
            if import_result.s3_deleted:
                console.print("[green]✓ S3 file deleted[/green]")

        # Show metadata verification result
        if import_result.metadata_verified:
            console.print("[green]✓ Metadata verified[/green]")
            vr = import_result.verification_result
            if vr:
                # Display verified metadata
                if vr.capture_date.original_value:
                    console.print(f"  Capture Date: {vr.capture_date.original_value}")
                if vr.gps_location.original_value:
                    console.print(f"  GPS Location: {vr.gps_location.original_value}")
                if vr.album_info.original_value:
                    albums_str = (
                        ", ".join(vr.album_info.original_value)
                        if vr.album_info.original_value
                        else "-"
                    )
                    console.print(f"  Albums: {albums_str}")
                # Show processing time proximity warning if present
                if vr.has_warning and vr.processing_time_warning:
                    console.print()
                    console.print(f"[yellow]⚠ {vr.processing_time_warning.message}[/yellow]")
        elif import_result.verification_skipped:
            console.print("[dim]Metadata verification skipped[/dim]")

        if import_result.albums:
            console.print(f"[green]✓ Added to albums: {', '.join(import_result.albums)}[/green]")

        # Show original deletion result
        if import_result.original_deleted:
            console.print(
                f"[green]✓ Original video moved to trash: {import_result.original_filename or 'original'}[/green]"
            )
        elif import_result.original_delete_error:
            console.print(
                f"[yellow]⚠ Failed to delete original: {import_result.original_delete_error}[/yellow]"
            )
    else:
        # Show metadata mismatch details if applicable
        if import_result.metadata_mismatch and import_result.verification_result:
            vr = import_result.verification_result
            console.print(f"[red]✗ Metadata mismatch: {import_result.converted_filename}[/red]")
            console.print()
            console.print("  [bold]Field          Original                    Converted[/bold]")
            console.print("  " + "─" * 55)

            # Capture date
            cd = vr.capture_date
            cd_status = "" if cd.matches else " ← MISMATCH"
            cd_orig = str(cd.original_value) if cd.original_value else "(none)"
            cd_conv = str(cd.converted_value) if cd.converted_value else "(missing)"
            console.print(f"  Capture Date   {cd_orig[:25]:<25} {cd_conv[:15]}{cd_status}")

            # GPS location
            gps = vr.gps_location
            gps_status = "" if gps.matches else " ← MISMATCH"
            gps_orig = str(gps.original_value) if gps.original_value else "(none)"
            gps_conv = str(gps.converted_value) if gps.converted_value else "(missing)"
            console.print(f"  GPS Location   {gps_orig[:25]:<25} {gps_conv[:15]}{gps_status}")

            # Albums
            alb = vr.album_info
            alb_orig = ", ".join(alb.original_value[:2]) if alb.original_value else "(none)"
            if alb.original_value and len(alb.original_value) > 2:
                alb_orig += f" (+{len(alb.original_value) - 2})"
            console.print(f"  Albums         {alb_orig[:25]:<25} (verified in JSON)")

            console.print()
            console.print("[yellow]Skipping import. Use --force to import anyway.[/yellow]")
        else:
            console.print(f"[red]✗ Import failed: {import_result.error_message}[/red]")
        sys.exit(1)


# Override import_cmd help text dynamically based on locale
import_cmd.help = get_help("import.description")


@cli.group(invoke_without_command=True)
@click.option("--json", "output_json", is_flag=True, help=get_help("config.json"))
@click.pass_context
def config(ctx, output_json: bool):
    """Display or modify configuration."""
    if ctx.invoked_subcommand is not None:
        return

    config_manager = ctx.obj["config"]
    all_config = config_manager.get_all()

    if output_json:
        click.echo(json.dumps(all_config, indent=2))
        return

    console.print("[bold]Current Configuration[/bold]")
    console.print()

    # AWS settings
    console.print("[cyan]AWS Settings:[/cyan]")
    console.print(f"  aws.region: {all_config['aws']['region']}")
    console.print(f"  aws.s3_bucket: {all_config['aws']['s3_bucket'] or '[not set]'}")
    console.print(f"  aws.role_arn: {all_config['aws']['role_arn'] or '[not set]'}")
    console.print()

    # Conversion settings
    console.print("[cyan]Conversion Settings:[/cyan]")
    console.print(f"  conversion.quality_preset: {all_config['conversion']['quality_preset']}")
    console.print(f"  conversion.max_concurrent: {all_config['conversion']['max_concurrent']}")
    console.print(f"  conversion.staging_folder: {all_config['conversion']['staging_folder']}")
    console.print(f"  conversion.download_timeout: {all_config['conversion']['download_timeout']}")
    console.print()

    console.print("[dim]Use 'vco config set <key> <value>' to change settings[/dim]")


# Override config help text dynamically based on locale
config.help = get_help("config.description")


@config.command("set")
@click.argument("key")
@click.argument("value")
@click.pass_context
def config_set(ctx, key: str, value: str):
    """Modify configuration value."""
    config_manager = ctx.obj["config"]

    try:
        # Convert value types
        converted_value: str | bool | int = value
        if value.lower() == "true":
            converted_value = True
        elif value.lower() == "false":
            converted_value = False
        elif value.isdigit():
            converted_value = int(value)

        config_manager.set(key, converted_value)
        config_manager.save()
        console.print(f"[green]✓ Set {key} = {converted_value}[/green]")
    except ValueError as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


# Override config_set help text dynamically based on locale
config_set.help = get_help("config.set.description")


def _download_icloud_videos(candidates, timeout: int, skip_confirm: bool) -> list:
    """Download iCloud videos before conversion.

    Args:
        candidates: List of ConversionCandidate with iCloud videos
        timeout: Download timeout in seconds
        skip_confirm: Skip confirmation prompt

    Returns:
        List of candidates that were successfully downloaded
    """
    from rich.progress import (
        BarColumn,
        Progress,
        SpinnerColumn,
        TextColumn,
        TimeElapsedColumn,
    )

    from vco.photos.swift_bridge import SwiftBridge
    from vco.services.icloud_download import (
        ICloudDownloadService,
    )

    # Calculate total download size
    total_size = sum(c.video.file_size for c in candidates)

    # Initialize SwiftBridge
    try:
        swift_bridge = SwiftBridge()
    except Exception as e:
        console.print(f"[red]Error: Swift bridge unavailable: {e}[/red]")
        return []

    # Initialize download service with progress callback
    current_task_id = None

    def progress_callback(progress_info):
        nonlocal current_task_id
        if current_task_id is not None:
            progress.update(
                current_task_id,
                completed=progress_info.downloaded_bytes,
                total=progress_info.total_bytes,
            )

    download_service = ICloudDownloadService(
        swift_bridge=swift_bridge,
        timeout=timeout,
        progress_callback=progress_callback,
    )

    # Check disk space
    has_space, available = download_service.check_disk_space(total_size)
    if not has_space:
        console.print()
        console.print("[red]Error: Insufficient disk space for iCloud downloads.[/red]")
        console.print(f"  Required: {format_size(total_size * 1.1)}")
        console.print(f"  Available: {format_size(available)}")
        return []

    # Show confirmation prompt
    console.print()
    console.print(f"[bold]iCloud videos to download: {len(candidates)}[/bold]")
    console.print(f"  Estimated size: {format_size(total_size)}")
    console.print(f"  Timeout: {timeout}s per video")
    console.print()

    if not skip_confirm:
        if not click.confirm("Download iCloud videos before conversion?"):
            console.print("[yellow]Skipping iCloud videos.[/yellow]")
            return []

    # Download with progress display
    console.print()
    console.print("[bold]Downloading iCloud videos...[/bold]")

    downloaded_candidates = []
    failed_downloads = []

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.fields[filename]}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TextColumn("{task.fields[size]}"),
        TimeElapsedColumn(),
        console=console,
    )

    with progress:
        for i, candidate in enumerate(candidates):
            video = candidate.video
            current_task_id = progress.add_task(
                "download",
                filename=f"[{i + 1}/{len(candidates)}] {video.filename[:30]}",
                size=format_size(video.file_size),
                total=video.file_size,
            )

            # Download
            result = download_service.download_video(video)

            if result.success and result.local_path:
                progress.update(current_task_id, completed=video.file_size)
                console.print(
                    f"  [green]✓[/green] {video.filename} ({result.download_time_seconds:.1f}s)"
                )
                # Update video path and is_local flag after successful download
                video.path = result.local_path
                video.is_local = True
                downloaded_candidates.append(candidate)
            else:
                progress.update(current_task_id, completed=video.file_size)
                console.print(f"  [red]✗[/red] {video.filename}: {result.error_message}")
                failed_downloads.append((video.filename, result.error_message))

            progress.remove_task(current_task_id)
            current_task_id = None

    # Show summary
    console.print()
    console.print("[bold]Download Summary[/bold]")
    console.print(f"  Successful: [green]{len(downloaded_candidates)}[/green]")
    console.print(f"  Failed: [red]{len(failed_downloads)}[/red]")

    if failed_downloads:
        console.print()
        console.print("[red]Failed downloads:[/red]")
        for filename, error in failed_downloads[:5]:
            console.print(f"  - {filename}: {error}")
        if len(failed_downloads) > 5:
            console.print(f"  ... and {len(failed_downloads) - 5} more")

    console.print()

    return downloaded_candidates


def _download_icloud_videos_parallel(
    candidates, timeout: int, concurrency_limit: int, skip_confirm: bool
) -> list:
    """Download iCloud videos in parallel before conversion.

    Args:
        candidates: List of ConversionCandidate with iCloud videos
        timeout: Download timeout in seconds
        concurrency_limit: Maximum concurrent downloads (1-10)
        skip_confirm: Skip confirmation prompt

    Returns:
        List of candidates that were successfully downloaded
    """
    from rich.progress import (
        BarColumn,
        Progress,
        SpinnerColumn,
        TextColumn,
        TimeElapsedColumn,
    )

    from vco.photos.swift_bridge import SwiftBridge
    from vco.services.icloud_download import ICloudDownloadService
    from vco.services.parallel_transfer import ParallelTransferService

    # Calculate total download size
    total_size = sum(c.video.file_size for c in candidates)

    # Initialize SwiftBridge
    try:
        swift_bridge = SwiftBridge()
    except Exception as e:
        console.print(f"[red]Error: Swift bridge unavailable: {e}[/red]")
        return []

    # Initialize download service (without progress callback - we'll handle it ourselves)
    download_service = ICloudDownloadService(
        swift_bridge=swift_bridge,
        timeout=timeout,
    )

    # Check disk space
    has_space, available = download_service.check_disk_space(total_size)
    if not has_space:
        console.print()
        console.print("[red]Error: Insufficient disk space for iCloud downloads.[/red]")
        console.print(f"  Required: {format_size(total_size * 1.1)}")
        console.print(f"  Available: {format_size(available)}")
        return []

    # Show confirmation prompt
    console.print()
    console.print(f"[bold]iCloud videos to download: {len(candidates)}[/bold]")
    console.print(f"  Estimated size: {format_size(total_size)}")
    console.print(f"  Timeout: {timeout}s per video")
    console.print(f"  Parallel downloads: {min(max(concurrency_limit, 1), 10)}")
    console.print()

    if not skip_confirm:
        if not click.confirm("Download iCloud videos before conversion?"):
            console.print("[yellow]Skipping iCloud videos.[/yellow]")
            return []

    # Download with progress display
    console.print()
    console.print("[bold]Downloading iCloud videos in parallel...[/bold]")

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.fields[filename]}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TextColumn("{task.fields[size]}"),
        TimeElapsedColumn(),
        console=console,
    )

    # Create download functions for each candidate
    candidate_map: dict[str, Any] = {}  # item_id -> candidate

    def create_download_func(candidate):
        """Create a download function for a candidate."""
        video = candidate.video

        def download():
            result = download_service.download_video(video)
            if result.success and result.local_path:
                # Update video path and is_local flag
                video.path = result.local_path
                video.is_local = True
                return result.local_path
            return None

        return download

    items = []
    for candidate in candidates:
        video = candidate.video
        item_id = video.uuid
        candidate_map[item_id] = candidate
        items.append((item_id, video.filename, create_download_func(candidate)))

    # Execute parallel downloads
    parallel_service = ParallelTransferService(
        concurrency_limit=concurrency_limit,
    )

    # Set up Ctrl+C handler
    import signal

    original_handler = signal.getsignal(signal.SIGINT)
    cancelled = False

    def handle_sigint(signum, frame):
        nonlocal cancelled
        cancelled = True
        console.print("\n[yellow]Cancelling downloads...[/yellow]")
        parallel_service.cancel()

    signal.signal(signal.SIGINT, handle_sigint)

    try:
        with progress:
            # Add overall progress task
            overall_task = progress.add_task(
                "download",
                filename=f"Downloading 0/{len(candidates)} files",
                size=format_size(total_size),
                total=len(candidates),
            )

            # Custom callback to update progress
            def update_progress(filename: str, percent: int, current: int, total: int) -> None:
                progress.update(
                    overall_task,
                    completed=current,
                    filename=f"Downloading {current}/{total} files",
                )

            parallel_service.progress_callback = update_progress
            summary = parallel_service.download_parallel(items)

            # Complete the progress bar
            progress.update(overall_task, completed=len(candidates))
    finally:
        # Restore original signal handler
        signal.signal(signal.SIGINT, original_handler)

    if cancelled:
        console.print("[yellow]Download cancelled by user.[/yellow]")
        console.print()

    # Collect successful downloads
    downloaded_candidates: list[Any] = []
    failed_downloads: list[tuple[str, str | None]] = []

    for result in summary.results:
        candidate = candidate_map.get(result.item_id)
        if candidate:
            if result.success:
                console.print(
                    f"  [green]✓[/green] {result.filename} ({result.transfer_time_seconds:.1f}s)"
                )
                downloaded_candidates.append(candidate)
            else:
                console.print(f"  [red]✗[/red] {result.filename}: {result.error_message}")
                failed_downloads.append((result.filename, result.error_message))

    # Show summary
    console.print()
    console.print("[bold]Download Summary[/bold]")
    console.print(f"  Successful: [green]{summary.successful}[/green]")
    console.print(f"  Failed: [red]{summary.failed}[/red]")
    console.print(f"  Total time: {summary.total_time_seconds:.1f}s")

    if failed_downloads:
        console.print()
        console.print("[red]Failed downloads:[/red]")
        for filename, error in failed_downloads[:5]:
            console.print(f"  - {filename}: {error}")
        if len(failed_downloads) > 5:
            console.print(f"  ... and {len(failed_downloads) - 5} more")

    console.print()

    return downloaded_candidates


def _convert_async_parallel(
    ctx, candidates, quality: str, aws_config, concurrency_limit: int, skip_confirm: bool = False
):
    """Handle async conversion mode with parallel uploads."""
    from rich.progress import (
        BarColumn,
        DownloadColumn,
        Progress,
        SpinnerColumn,
        TextColumn,
        TransferSpeedColumn,
    )

    from vco.services.async_convert import AsyncConvertCommand

    # Get API URL from config or use default
    api_url = getattr(aws_config, "async_api_url", None)
    if not api_url:
        # Use default API URL based on region
        api_url = f"https://dln48ri1di.execute-api.{aws_config.region}.amazonaws.com/dev"

    console.print()
    console.print("[bold]Async conversion mode[/bold]")
    console.print(f"API URL: {api_url}")
    console.print(f"Parallel uploads: {min(max(concurrency_limit, 1), 10)}")

    try:
        async_cmd = AsyncConvertCommand(
            api_url=api_url,
            s3_bucket=aws_config.s3_bucket,
            region=aws_config.region,
            profile_name=aws_config.profile or None,
        )

        console.print("[bold]Uploading files in parallel...[/bold]")

        # Use Rich Progress for upload progress display
        progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.fields[filename]}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            DownloadColumn(),
            TransferSpeedColumn(),
            console=console,
        )

        # Execute parallel upload using AsyncConvertCommand's parallel method
        with progress:
            overall_task = progress.add_task(
                "upload",
                filename=f"Uploading 0/{len(candidates)} files",
                total=len(candidates),
            )

            def upload_progress_callback(
                filename: str, percent: int, current: int, total: int
            ) -> None:
                progress.update(
                    overall_task,
                    completed=current,
                    filename=f"Uploading {current}/{total} files",
                )

            result = async_cmd.execute_parallel(
                candidates=candidates,
                quality_preset=quality,
                concurrency_limit=concurrency_limit,
                progress_callback=upload_progress_callback,
            )

            # Complete the progress bar
            progress.update(overall_task, completed=len(candidates))

        if result.status == "ERROR":
            console.print(f"[red]✗ Failed: {result.error_message}[/red]")
            sys.exit(1)

        console.print()
        console.print("[green]✓ Task submitted successfully[/green]")
        console.print(f"  Task ID: [cyan]{result.task_id}[/cyan]")
        console.print(f"  Files: {result.file_count}")
        console.print()
        console.print("[dim]Use 'vco status' to check progress[/dim]")
        console.print(f"[dim]Use 'vco status {result.task_id}' for details[/dim]")

    except Exception as e:
        console.print(f"[red]✗ Failed to submit task: {e}[/red]")
        sys.exit(1)


def _convert_async(ctx, candidates, quality: str, aws_config, skip_confirm: bool = False):
    """Handle async conversion mode."""
    from rich.progress import (
        BarColumn,
        DownloadColumn,
        Progress,
        SpinnerColumn,
        TextColumn,
        TransferSpeedColumn,
    )

    from vco.services.async_convert import AsyncConvertCommand, UploadProgress

    # Get API URL from config or use default
    api_url = getattr(aws_config, "async_api_url", None)
    if not api_url:
        # Use default API URL based on region
        api_url = f"https://dln48ri1di.execute-api.{aws_config.region}.amazonaws.com/dev"

    console.print()
    console.print("[bold]Async conversion mode[/bold]")
    console.print(f"API URL: {api_url}")

    # Use Rich Progress for upload progress display
    progress = Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.fields[filename]}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        DownloadColumn(),
        TransferSpeedColumn(),
        console=console,
    )

    # Track current task for progress updates
    current_task_id = None
    current_filename = None

    def progress_callback(upload_progress: UploadProgress):
        nonlocal current_task_id, current_filename

        # Create new task if filename changed
        if upload_progress.filename != current_filename:
            if current_task_id is not None:
                # Complete previous task
                progress.update(current_task_id, completed=progress.tasks[current_task_id].total)

            current_filename = upload_progress.filename
            current_task_id = progress.add_task(
                "upload",
                filename=upload_progress.filename,
                total=upload_progress.total_bytes,
            )

        # Update progress
        if current_task_id is not None:
            progress.update(current_task_id, completed=upload_progress.uploaded_bytes)

    try:
        async_cmd = AsyncConvertCommand(
            api_url=api_url,
            s3_bucket=aws_config.s3_bucket,
            region=aws_config.region,
            profile_name=aws_config.profile or None,
            progress_callback=progress_callback,
        )

        console.print("[bold]Uploading files...[/bold]")

        with progress:
            result = async_cmd.execute(
                candidates=candidates,
                quality_preset=quality,
            )
            # Complete the last file's progress before exiting the progress context
            if current_task_id is not None:
                progress.update(current_task_id, completed=progress.tasks[current_task_id].total)

        if result.status == "ERROR":
            console.print(f"[red]✗ Failed: {result.error_message}[/red]")
            sys.exit(1)

        console.print()
        console.print("[green]✓ Task submitted successfully[/green]")
        console.print(f"  Task ID: [cyan]{result.task_id}[/cyan]")
        console.print(f"  Files: {result.file_count}")
        console.print()
        console.print("[dim]Use 'vco status' to check progress[/dim]")
        console.print(f"[dim]Use 'vco status {result.task_id}' for details[/dim]")

    except Exception as e:
        console.print(f"[red]✗ Failed to submit task: {e}[/red]")
        sys.exit(1)


@cli.command()
@click.option("--filter", "status_filter", help=get_help("status.filter"))
@click.option("--limit", "-n", type=int, default=10, help=get_help("status.limit"))
@click.option("--json", "output_json", is_flag=True, help=get_help("status.json"))
@click.argument("task_id", required=False)
@click.pass_context
def status(ctx, status_filter: str | None, limit: int, output_json: bool, task_id: str | None):
    """Check async task status."""
    from vco.services.async_status import StatusCommand

    config = ctx.obj["config"]
    aws_config = config.config.aws

    # Get API URL
    api_url = getattr(aws_config, "async_api_url", None)
    if not api_url:
        api_url = f"https://dln48ri1di.execute-api.{aws_config.region}.amazonaws.com/dev"

    try:
        status_cmd = StatusCommand(
            api_url=api_url,
            region=aws_config.region,
            profile_name=aws_config.profile or None,
        )

        if task_id:
            # Show task details
            task = status_cmd.get_task_detail(task_id)

            if output_json:
                click.echo(
                    json.dumps(
                        {
                            "task_id": task.task_id,
                            "status": task.status,
                            "quality_preset": task.quality_preset,
                            "progress_percentage": task.progress_percentage,
                            "current_step": task.current_step,
                            "created_at": task.created_at.isoformat(),
                            "files": [
                                {
                                    "file_id": f.file_id,
                                    "filename": f.filename,
                                    "status": f.status,
                                    "progress_percentage": f.progress_percentage,
                                    "ssim_score": f.ssim_score,
                                    "vmaf_score": f.vmaf_score,
                                    "error_message": f.error_message,
                                }
                                for f in task.files
                            ],
                        },
                        indent=2,
                    )
                )
                return

            # Display task details
            console.print(f"[bold]Task: {task.task_id}[/bold]")
            console.print()
            console.print(f"  Status: {_format_status(task.status)}")
            console.print(f"  Quality: {task.quality_preset}")
            console.print(f"  Progress: {task.progress_percentage}%")
            if task.current_step:
                console.print(f"  Current Step: {task.current_step}")
            local_created = utc_to_local(task.created_at)
            console.print(f"  Created: {local_created.strftime('%Y-%m-%d %H:%M:%S')}")
            if task.estimated_completion_time:
                local_est = utc_to_local(task.estimated_completion_time)
                console.print(f"  Est. Completion: {local_est.strftime('%H:%M:%S')}")
            console.print()

            # Display files
            table = Table(title="Files")
            table.add_column("Filename", style="cyan")
            table.add_column("Status")
            table.add_column("Progress")
            table.add_column("SSIM")
            table.add_column("VMAF")
            table.add_column("Error", style="red")

            for f in task.files:
                table.add_row(
                    f.filename[:30] + ("..." if len(f.filename) > 30 else ""),
                    _format_status(f.status),
                    f"{f.progress_percentage}%",
                    f"{f.ssim_score:.4f}" if f.ssim_score else "-",
                    f"{f.vmaf_score:.2f}" if f.vmaf_score else "-",
                    f.error_message[:30] if f.error_message else "-",
                )

            console.print(table)

        else:
            # List tasks
            tasks = status_cmd.list_tasks(status_filter=status_filter, limit=limit)

            if output_json:
                click.echo(
                    json.dumps(
                        {
                            "tasks": [
                                {
                                    "task_id": t.task_id,
                                    "status": t.status,
                                    "file_count": t.file_count,
                                    "completed_count": t.completed_count,
                                    "failed_count": t.failed_count,
                                    "progress_percentage": t.progress_percentage,
                                    "created_at": t.created_at.isoformat(),
                                }
                                for t in tasks
                            ]
                        },
                        indent=2,
                    )
                )
                return

            if not tasks:
                console.print("[green]No active tasks.[/green]")
                return

            console.print("[bold]Recent Tasks:[/bold]")
            console.print()

            table = Table()
            table.add_column("Task ID", style="cyan")
            table.add_column("Status")
            table.add_column("Files")
            table.add_column("Progress")
            table.add_column("Created")

            for t in tasks:
                files_str = f"{t.completed_count}/{t.file_count}"
                if t.failed_count > 0:
                    files_str += f" ([red]{t.failed_count} failed[/red])"

                local_created = utc_to_local(t.created_at)
                table.add_row(
                    t.task_id,
                    _format_status(t.status),
                    files_str,
                    f"{t.progress_percentage}%",
                    local_created.strftime("%m-%d %H:%M"),
                )

            console.print(table)
            console.print()
            console.print("[dim]Use 'vco status <task_id>' for details[/dim]")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


# Override status help text dynamically based on locale
status.help = get_help("status.description")


@cli.command()
@click.option("--yes", "-y", is_flag=True, help=get_help("cancel.yes"))
@click.argument("task_id")
@click.pass_context
def cancel(ctx, yes: bool, task_id: str):
    """Cancel a running async task."""
    from vco.services.async_cancel import CancelCommand

    config = ctx.obj["config"]
    aws_config = config.config.aws

    # Get API URL
    api_url = getattr(aws_config, "async_api_url", None)
    if not api_url:
        api_url = f"https://dln48ri1di.execute-api.{aws_config.region}.amazonaws.com/dev"

    console.print(f"[bold]Cancelling task: {task_id}[/bold]")

    if not yes and not click.confirm("Are you sure you want to cancel this task?"):
        console.print("Cancelled.")
        return

    try:
        cancel_cmd = CancelCommand(
            api_url=api_url,
            region=aws_config.region,
            profile_name=aws_config.profile or None,
        )

        result = cancel_cmd.cancel(task_id)

        if result.success:
            console.print("[green]✓ Task cancelled successfully[/green]")
            console.print(f"  Previous status: {result.previous_status}")
            if result.s3_files_deleted:
                console.print("  [green]✓ S3 files cleaned up[/green]")
            if result.mediaconvert_cancelled:
                console.print("  [green]✓ MediaConvert jobs cancelled[/green]")
        else:
            console.print(f"[red]✗ Failed to cancel: {result.error_message}[/red]")
            sys.exit(1)

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


# Override cancel help text dynamically based on locale
cancel.help = get_help("cancel.description")


def _format_status(status: str) -> str:
    """Format status with color."""
    # Note: VERIFYING status removed - quality evaluation now in CONVERTING phase
    status_colors = {
        "PENDING": "[yellow]PENDING[/yellow]",
        "UPLOADING": "[blue]UPLOADING[/blue]",
        "CONVERTING": "[blue]CONVERTING[/blue]",
        "COMPLETED": "[green]COMPLETED[/green]",
        "PARTIALLY_COMPLETED": "[yellow]PARTIALLY_COMPLETED[/yellow]",
        "FAILED": "[red]FAILED[/red]",
        "CANCELLED": "[dim]CANCELLED[/dim]",
        "PROCESSING": "[blue]PROCESSING[/blue]",
    }
    return status_colors.get(status, status)


def main():
    """Entry point for the CLI."""
    try:
        cli(obj={})
    except KeyboardInterrupt:
        console.print("\n[yellow]Cancelled.[/yellow]")
        sys.exit(130)


if __name__ == "__main__":
    main()
