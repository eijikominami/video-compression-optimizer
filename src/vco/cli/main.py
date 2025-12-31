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
from datetime import datetime
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

from vco.analyzer.analyzer import CompressionAnalyzer
from vco.cli.i18n import get_help
from vco.config.manager import ConfigManager
from vco.converter.mediaconvert import MediaConvertClient
from vco.metadata.manager import MetadataManager
from vco.photos.manager import PhotosAccessManager
from vco.quality.checker import QualityChecker
from vco.services.convert import ConversionProgress, ConvertService
from vco.services.import_service import ImportService
from vco.services.review import ReviewService
from vco.services.scan import ScanService

console = Console()


def format_size(size_bytes: int) -> str:
    """Format bytes as human-readable size."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(size_bytes) < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} PB"


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
def scan(ctx, from_date: str | None, to_date: str | None, top_n: int | None, output_json: bool):
    """Scan Apple Photos library and display conversion candidates."""
    config = ctx.obj["config"]

    # Parse dates
    from_dt = parse_date(from_date) if from_date else None
    to_dt = parse_date(to_date) if to_date else None

    # Initialize services
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
    console.print(
        f"  Estimated savings: {format_size(summary.estimated_total_savings_bytes)} ({summary.estimated_total_savings_percent:.1f}%)"
    )
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
    table.add_column("Est. Savings", style="green")

    for candidate in result.candidates[:20]:  # Show first 20
        video = candidate.video

        # Determine location status
        if video.is_local:
            location = "Local"
        else:
            location = "iCloud"

        table.add_row(
            video.filename[:40] + ("..." if len(video.filename) > 40 else ""),
            video.codec,
            f"{video.resolution[0]}x{video.resolution[1]}",
            format_duration(video.duration),
            format_size(video.file_size),
            location,
            f"{format_size(candidate.estimated_savings_bytes)} ({candidate.estimated_savings_percent:.0f}%)",
        )

    console.print(table)

    if len(result.candidates) > 20:
        console.print(f"[dim]... and {len(result.candidates) - 20} more candidates[/dim]")

    # Show iCloud-only videos
    icloud_only = [c for c in result.candidates if not c.video.is_local]
    if icloud_only:
        console.print()
        console.print(
            f"[yellow]⚠ {len(icloud_only)} videos are in iCloud only and need to be downloaded first.[/yellow]"
        )
        console.print(
            "[dim]Open Photos app and download these videos, then run 'vco scan' again:[/dim]"
        )
        console.print()

        for candidate in icloud_only[:10]:  # Show first 10 iCloud-only
            video = candidate.video
            console.print(f"  - {video.filename}")

        if len(icloud_only) > 10:
            console.print(f"[dim]  ... and {len(icloud_only) - 10} more iCloud-only videos[/dim]")

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
@click.option("--async", "async_mode", is_flag=True, help=get_help("convert.async"))
@click.pass_context
def convert(ctx, quality: str, top_n: int | None, dry_run: bool, async_mode: bool):
    """Convert candidate videos to H.265."""
    config = ctx.obj["config"]

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

    console.print(f"[bold]Found {len(candidates_to_convert)} candidates for conversion[/bold]")
    console.print(f"Quality preset: {quality}")

    if dry_run:
        console.print("[yellow]Dry run mode - no actual conversion will be performed[/yellow]")
        console.print()

        # Show what would be converted
        table = Table(title="Would Convert")
        table.add_column("Filename", style="cyan")
        table.add_column("Size")
        table.add_column("Est. Savings", style="green")

        for candidate in candidates_to_convert:
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

    # Handle async mode
    if async_mode:
        _convert_async(ctx, candidates_to_convert, quality, aws_config)
        return

    console.print()
    console.print("[yellow]Conversion requires AWS credentials and will incur costs.[/yellow]")
    console.print("[yellow]Use 'vco convert --dry-run' to preview without converting.[/yellow]")

    if not click.confirm("Do you want to proceed?"):
        console.print("Cancelled.")
        return

    # Initialize services
    try:
        mediaconvert_client = MediaConvertClient(
            region=aws_config.region,
            s3_bucket=aws_config.s3_bucket,
            role_arn=aws_config.role_arn,
            profile_name=aws_config.profile,
        )
    except Exception as e:
        console.print(f"[red]Failed to initialize AWS client: {e}[/red]")
        sys.exit(1)

    quality_checker = QualityChecker(
        region=aws_config.region,
        s3_bucket=aws_config.s3_bucket,
        lambda_function_name=aws_config.quality_checker_function or "vco-quality-checker-dev",
        profile_name=aws_config.profile,
    )

    metadata_manager = MetadataManager()

    # Initialize PhotosAccessManager for iCloud downloads
    photos_manager = PhotosAccessManager()

    staging_folder = Path(config.get("conversion.staging_folder")).expanduser()

    # Progress callback
    current_progress = {}

    def progress_callback(progress: ConversionProgress):
        current_progress[progress.uuid] = progress
        status_icon = {
            "uploading": "⬆️",
            "downloading_icloud": "☁️",
            "converting": "🔄",
            "checking": "✅",
            "downloading": "⬇️",
            "complete": "✓",
            "failed": "✗",
        }.get(progress.stage, "?")
        console.print(
            f"  {status_icon} {progress.filename}: {progress.stage} ({progress.progress_percent}%)"
        )

    # Initialize ReviewService for auto-registration
    review_service = ReviewService()

    convert_service = ConvertService(
        mediaconvert_client=mediaconvert_client,
        quality_checker=quality_checker,
        metadata_manager=metadata_manager,
        photos_manager=photos_manager,
        staging_folder=staging_folder,
        progress_callback=progress_callback,
        review_service=review_service,
    )

    # Estimate cost
    estimated_cost = convert_service.estimate_batch_cost(candidates_to_convert)
    console.print(f"[yellow]Estimated AWS cost: ${estimated_cost:.2f}[/yellow]")
    console.print()

    # Run conversion
    console.print("[bold]Starting conversion...[/bold]")
    console.print()

    try:
        batch_result = convert_service.convert_batch(
            candidates=candidates_to_convert,
            quality_preset=quality,
            max_concurrent=config.get("conversion.max_concurrent"),
        )
    except Exception as e:
        console.print(f"[red]Conversion failed: {e}[/red]")
        sys.exit(1)

    # Display results
    console.print()
    console.print("[bold]Conversion Complete[/bold]")
    console.print(f"  Total: {batch_result.total}")
    console.print(f"  Successful: [green]{batch_result.successful}[/green]")
    console.print(f"  Failed: [red]{batch_result.failed}[/red]")
    console.print(f"  Added to review queue: [cyan]{batch_result.added_to_queue}[/cyan]")

    # Display best-effort mode results
    best_effort_results = [r for r in batch_result.results if r.best_effort]
    if best_effort_results:
        console.print()
        console.print("[yellow]Best-effort mode used:[/yellow]")
        for result in best_effort_results:
            ssim = result.quality_result.ssim_score if result.quality_result else "N/A"
            ssim_str = f"{ssim:.4f}" if isinstance(ssim, float) else ssim
            console.print(
                f"  - {result.filename}: preset={result.selected_preset}, SSIM={ssim_str}"
            )

    if batch_result.errors:
        console.print()
        console.print("[red]Errors:[/red]")
        for error in batch_result.errors:
            console.print(f"  - {error}")

    if batch_result.successful > 0:
        console.print()
        console.print(f"[green]Converted files saved to: {staging_folder}[/green]")
        console.print("[dim]Run 'vco import --list' to see pending imports[/dim]")


# Override convert help text dynamically based on locale
convert.help = get_help("convert.description")


@cli.command("import")
@click.option("--list", "list_mode", is_flag=True, help=get_help("import.list"))
@click.option("--all", "all_mode", is_flag=True, help=get_help("import.all"))
@click.option("--clear", "clear_mode", is_flag=True, help=get_help("import.clear"))
@click.option("--remove", "remove_id", help=get_help("import.remove"))
@click.option("--json", "output_json", is_flag=True, help=get_help("import.json"))
@click.argument("review_id", required=False)
@click.pass_context
def import_cmd(
    ctx,
    list_mode: bool,
    all_mode: bool,
    clear_mode: bool,
    remove_id: str | None,
    output_json: bool,
    review_id: str | None,
):
    """Import converted videos to Photos library."""
    import_service = ImportService()

    # --clear mode
    if clear_mode:
        pending = import_service.list_pending()
        if not pending:
            console.print("[green]Review queue is already empty.[/green]")
            return

        console.print(
            f"[yellow]Will remove {len(pending)} items and files from review queue.[/yellow]"
        )
        if not click.confirm("Do you want to proceed?"):
            console.print("Cancelled.")
            return

        result = import_service.clear_queue()
        if result.success:
            console.print(f"[green]✓ Removed {result.items_removed} items.[/green]")
            console.print(f"[green]✓ Deleted {result.files_deleted} files.[/green]")

            if result.files_failed > 0:
                console.print(f"[yellow]⚠ {result.files_failed} file deletions failed.[/yellow]")
                for error in result.error_details[:3]:  # Show first 3 errors
                    console.print(f"  {error}")
                if len(result.error_details) > 3:
                    console.print(f"  ... and {len(result.error_details) - 3} more errors")
        else:
            console.print("[red]✗ Failed to clear queue.[/red]")
            sys.exit(1)
        return

    # --remove mode
    if remove_id:
        result = import_service.remove_item(remove_id)
        if result.success:
            console.print(f"[green]✓ Removed {remove_id} from review queue.[/green]")

            if result.files_deleted:
                file_status = []
                if result.files_deleted.video_deleted:
                    file_status.append("video file")
                if result.files_deleted.metadata_deleted:
                    file_status.append("metadata file")

                if file_status:
                    console.print(f"[green]✓ Deleted {', '.join(file_status)}.[/green]")

                # Report any file deletion errors
                if result.files_deleted.video_error or result.files_deleted.metadata_error:
                    console.print("[yellow]⚠ Some file deletions failed:[/yellow]")
                    if result.files_deleted.video_error:
                        console.print(f"  Video: {result.files_deleted.video_error}")
                    if result.files_deleted.metadata_error:
                        console.print(f"  Metadata: {result.files_deleted.metadata_error}")
        else:
            console.print(f"[red]✗ Failed to remove: {result.error_message}[/red]")
            sys.exit(1)
        return

    # --list mode
    if list_mode:
        pending = import_service.list_pending()

        if output_json:
            items = [item.to_dict() for item in pending]
            click.echo(json.dumps({"items": items}, indent=2))
            return

        if not pending:
            console.print("[green]No pending imports.[/green]")
            return

        console.print(f"[bold]Pending imports: {len(pending)}[/bold]")
        console.print()

        table = Table(title="Pending Imports")
        table.add_column("ID", style="cyan")
        table.add_column("Filename")
        table.add_column("Original")
        table.add_column("Converted")
        table.add_column("Savings", style="green")
        table.add_column("SSIM")
        table.add_column("Albums", style="magenta")

        for item in pending:
            qr = item.quality_result
            original_size = qr.get("original_size", 0)
            converted_size = qr.get("converted_size", 0)
            savings = qr.get("space_saved_bytes", 0)
            ssim = qr.get("ssim_score", 0)
            albums = item.metadata.get("albums", [])
            albums_str = ", ".join(albums[:3]) if albums else "-"
            if len(albums) > 3:
                albums_str += f" (+{len(albums) - 3})"

            table.add_row(
                item.id,
                item.converted_path.name[:25]
                + ("..." if len(item.converted_path.name) > 25 else ""),
                format_size(original_size),
                format_size(converted_size),
                format_size(savings),
                f"{ssim:.4f}" if ssim else "N/A",
                albums_str,
            )

        console.print(table)
        console.print()
        console.print(
            "[dim]Use 'vco import <id>' for single import, 'vco import --all' for batch import[/dim]"
        )
        return

    # --all mode
    if all_mode:
        pending = import_service.list_pending()

        if not pending:
            console.print("[green]No pending imports.[/green]")
            return

        console.print(f"[bold]Importing {len(pending)} videos in batch[/bold]")
        console.print()
        console.print(
            "[yellow]Note: After import, manually delete original videos in Photos app.[/yellow]"
        )
        console.print()

        if not click.confirm("Do you want to proceed?"):
            console.print("Cancelled.")
            return

        console.print()
        console.print("[bold]Importing...[/bold]")

        result = import_service.import_all()

        if output_json:
            click.echo(
                json.dumps(
                    {
                        "total": result.total,
                        "successful": result.successful,
                        "failed": result.failed,
                        "results": [
                            {
                                "success": r.success,
                                "review_id": r.review_id,
                                "original_filename": r.original_filename,
                                "converted_filename": r.converted_filename,
                                "albums": r.albums,
                                "error_message": r.error_message,
                            }
                            for r in result.results
                        ],
                    },
                    indent=2,
                )
            )
            return

        console.print()
        console.print("[bold]Import Complete[/bold]")
        console.print(f"  Total: {result.total}")
        console.print(f"  Successful: [green]{result.successful}[/green]")
        console.print(f"  Failed: [red]{result.failed}[/red]")

        if result.failed > 0:
            console.print()
            console.print("[red]Errors:[/red]")
            for r in result.results:
                if not r.success:
                    console.print(f"  - {r.original_filename}: {r.error_message}")

        if result.successful > 0:
            console.print()
            console.print("[yellow]⚠ Manually delete original videos in Photos app.[/yellow]")

        return

    # Single import mode
    if not review_id:
        console.print("[red]Error: Specify review_id or use --list or --all option.[/red]")
        console.print()
        console.print("Usage:")
        console.print("  vco import --list        List pending imports")
        console.print("  vco import <review_id>   Import specified video")
        console.print("  vco import --all         Import all videos")
        sys.exit(1)

    # Get item info first
    item = import_service.review_service.get_review_by_id(review_id)
    if item is None:
        console.print(f"[red]Error: ID not found: {review_id}[/red]")
        sys.exit(1)

    if item.status != "pending_review":
        console.print(f"[yellow]This item has already been processed: {item.status}[/yellow]")
        sys.exit(1)

    albums = item.metadata.get("albums", [])

    console.print(f"[bold]Import: {item.converted_path.name}[/bold]")
    console.print()
    console.print("Actions:")
    console.print("  1. Import converted video to Photos")
    if albums:
        console.print(f"  2. Add to albums: {', '.join(albums)}")
    console.print()
    console.print(
        "[yellow]Note: After import, manually delete original video in Photos app.[/yellow]"
    )
    console.print()

    if not click.confirm("Do you want to proceed?"):
        console.print("Cancelled.")
        return

    result = import_service.import_single(review_id)

    if output_json:
        click.echo(
            json.dumps(
                {
                    "success": result.success,
                    "review_id": result.review_id,
                    "original_filename": result.original_filename,
                    "converted_filename": result.converted_filename,
                    "albums": result.albums,
                    "error_message": result.error_message,
                },
                indent=2,
            )
        )
        return

    if result.success:
        console.print("[green]✓ Import to Photos completed[/green]")
        if result.albums:
            console.print(f"[green]✓ Added to albums: {', '.join(result.albums)}[/green]")
        console.print()
        console.print("[yellow]⚠ Manually delete original video in Photos app.[/yellow]")
    else:
        console.print(f"[red]✗ Import failed: {result.error_message}[/red]")
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
        if value.lower() == "true":
            value = True
        elif value.lower() == "false":
            value = False
        elif value.isdigit():
            value = int(value)

        config_manager.set(key, value)
        config_manager.save()
        console.print(f"[green]✓ Set {key} = {value}[/green]")
    except ValueError as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


# Override config_set help text dynamically based on locale
config_set.help = get_help("config.set.description")


def _convert_async(ctx, candidates, quality: str, aws_config):
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
    console.print()
    console.print("[yellow]Files will be uploaded to S3 and processed asynchronously.[/yellow]")
    console.print("[yellow]Use 'vco status' to check progress.[/yellow]")
    console.print()

    if not click.confirm("Do you want to proceed?"):
        console.print("Cancelled.")
        return

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
@click.option("--json", "output_json", is_flag=True, help=get_help("status.json"))
@click.argument("task_id", required=False)
@click.pass_context
def status(ctx, status_filter: str | None, output_json: bool, task_id: str | None):
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
            console.print(f"  Created: {task.created_at.strftime('%Y-%m-%d %H:%M:%S')}")
            if task.estimated_completion_time:
                console.print(
                    f"  Est. Completion: {task.estimated_completion_time.strftime('%H:%M:%S')}"
                )
            console.print()

            # Display files
            table = Table(title="Files")
            table.add_column("Filename", style="cyan")
            table.add_column("Status")
            table.add_column("Progress")
            table.add_column("SSIM")
            table.add_column("Error", style="red")

            for f in task.files:
                table.add_row(
                    f.filename[:30] + ("..." if len(f.filename) > 30 else ""),
                    _format_status(f.status),
                    f"{f.progress_percentage}%",
                    f"{f.ssim_score:.4f}" if f.ssim_score else "-",
                    f.error_message[:30] if f.error_message else "-",
                )

            console.print(table)

        else:
            # List tasks
            tasks = status_cmd.list_tasks(status_filter=status_filter)

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

            console.print(f"[bold]Active Tasks: {len(tasks)}[/bold]")
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

                table.add_row(
                    t.task_id[:8] + "...",
                    _format_status(t.status),
                    files_str,
                    f"{t.progress_percentage}%",
                    t.created_at.strftime("%m-%d %H:%M"),
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
@click.argument("task_id")
@click.pass_context
def cancel(ctx, task_id: str):
    """Cancel a running async task."""
    from vco.services.async_cancel import CancelCommand

    config = ctx.obj["config"]
    aws_config = config.config.aws

    # Get API URL
    api_url = getattr(aws_config, "async_api_url", None)
    if not api_url:
        api_url = f"https://dln48ri1di.execute-api.{aws_config.region}.amazonaws.com/dev"

    console.print(f"[bold]Cancelling task: {task_id}[/bold]")

    if not click.confirm("Are you sure you want to cancel this task?"):
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


@cli.command()
@click.argument("task_id")
@click.option("--output", "-o", "output_dir", type=click.Path(), help=get_help("download.output"))
@click.option("--no-resume", is_flag=True, help=get_help("download.no_resume"))
@click.option("--json", "output_json", is_flag=True, help=get_help("download.json"))
@click.pass_context
def download(ctx, task_id: str, output_dir: str | None, no_resume: bool, output_json: bool):
    """Download results from completed async tasks."""
    from rich.progress import (
        BarColumn,
        DownloadColumn,
        Progress,
        SpinnerColumn,
        TextColumn,
        TransferSpeedColumn,
    )

    from vco.services.async_download import DownloadCommand

    config = ctx.obj["config"]
    aws_config = config.config.aws

    # Get API URL
    api_url = getattr(aws_config, "async_api_url", None)
    if not api_url:
        api_url = f"https://dln48ri1di.execute-api.{aws_config.region}.amazonaws.com/dev"

    # Determine output directory
    if output_dir:
        output_path = Path(output_dir)
    else:
        output_path = Path(config.get("conversion.staging_folder")).expanduser()

    # Use Rich Progress for download progress display
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

    def progress_callback(filename, percent, downloaded, total):
        nonlocal current_task_id, current_filename

        # Create new task if filename changed
        if filename != current_filename:
            if current_task_id is not None:
                # Complete previous task
                progress.update(current_task_id, completed=progress.tasks[current_task_id].total)

            current_filename = filename
            current_task_id = progress.add_task(
                "download",
                filename=filename,
                total=total,
            )

        # Update progress
        if current_task_id is not None:
            progress.update(current_task_id, completed=downloaded)

    try:
        download_cmd = DownloadCommand(
            api_url=api_url,
            s3_bucket=aws_config.s3_bucket,
            region=aws_config.region,
            profile_name=aws_config.profile or None,
            output_dir=output_path,
            progress_callback=progress_callback,
        )

        console.print(f"[bold]Downloading task: {task_id}[/bold]")
        console.print(f"Output directory: {output_path}")
        console.print()

        with progress:
            result = download_cmd.download(
                task_id=task_id,
                resume=not no_resume,
            )

        if output_json:
            click.echo(
                json.dumps(
                    {
                        "task_id": result.task_id,
                        "success": result.success,
                        "total_files": result.total_files,
                        "downloaded_files": result.downloaded_files,
                        "failed_files": result.failed_files,
                        "added_to_queue": result.added_to_queue,
                        "results": [
                            {
                                "file_id": r.file_id,
                                "filename": r.filename,
                                "success": r.success,
                                "local_path": str(r.local_path) if r.local_path else None,
                                "error_message": r.error_message,
                            }
                            for r in result.results
                        ],
                    },
                    indent=2,
                )
            )
            return

        console.print()
        console.print("[bold]Download Complete[/bold]")
        console.print(f"  Total files: {result.total_files}")
        console.print(f"  Downloaded: [green]{result.downloaded_files}[/green]")
        console.print(f"  Failed: [red]{result.failed_files}[/red]")
        console.print(f"  Added to review queue: [cyan]{result.added_to_queue}[/cyan]")

        if result.failed_files > 0:
            console.print()
            console.print("[red]Errors:[/red]")
            for r in result.results:
                if not r.success:
                    console.print(f"  - {r.filename}: {r.error_message}")

        if result.downloaded_files > 0:
            console.print()
            console.print(f"[green]Files saved to: {output_path}[/green]")
            console.print("[dim]Run 'vco import --list' to see pending imports[/dim]")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


# Override download help text dynamically based on locale
download.help = get_help("download.description")


def _format_status(status: str) -> str:
    """Format status with color."""
    status_colors = {
        "PENDING": "[yellow]PENDING[/yellow]",
        "UPLOADING": "[blue]UPLOADING[/blue]",
        "CONVERTING": "[blue]CONVERTING[/blue]",
        "VERIFYING": "[blue]VERIFYING[/blue]",
        "COMPLETED": "[green]COMPLETED[/green]",
        "PARTIALLY_COMPLETED": "[yellow]PARTIALLY_COMPLETED[/yellow]",
        "FAILED": "[red]FAILED[/red]",
        "CANCELLED": "[dim]CANCELLED[/dim]",
        "PROCESSING": "[blue]PROCESSING[/blue]",
    }
    return status_colors.get(status, status)


def main():
    """Entry point for the CLI."""
    cli(obj={})


if __name__ == "__main__":
    main()
