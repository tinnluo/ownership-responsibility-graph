import subprocess
import sys


def test_cli_pipeline_runs_end_to_end(tmp_path, read_csv):
    output_dir = tmp_path / "output"

    commands = [
        [
            sys.executable,
            "scripts/build_demo_graph.py",
            "--data-dir",
            "data/sample",
            "--output-dir",
            str(output_dir),
        ],
        [
            sys.executable,
            "scripts/run_analysis.py",
            "--data-dir",
            "data/sample",
            "--output-dir",
            str(output_dir),
            "--root-entity-id",
            "ENT_001",
            "--asset-id",
            "AST_100",
        ],
        [
            sys.executable,
            "scripts/export_neo4j.py",
            "--input-dir",
            str(output_dir),
            "--output-dir",
            str(output_dir / "neo4j"),
        ],
    ]

    for command in commands:
        result = subprocess.run(command, capture_output=True, text=True, check=False)
        assert result.returncode == 0, result.stderr

    ranking = read_csv(output_dir / "analysis" / "top_responsible_entities.csv")
    attributed = read_csv(output_dir / "analysis" / "attributed_emission_relationships.csv")
    assert ranking.iloc[0]["root_entity_id"] == "ENT_040"
    assert ranking.iloc[0]["total_attributed_emissions"] == 557.5
    assert len(attributed) == 5
