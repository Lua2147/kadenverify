import importlib.util
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "drive_parallel_intake_scan.py"
SPEC = importlib.util.spec_from_file_location("drive_parallel_intake_scan", MODULE_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
is_excluded_vertical = MODULE.is_excluded_vertical


def test_excludes_higher_ed_dot_edu_domain() -> None:
    assert (
        is_excluded_vertical(
            company="Stanford University",
            industry="Higher Education",
            naics_text="",
            company_type="",
            domain="stanford.edu",
            source_name="contacts.csv",
        )
        is True
    )


def test_excludes_higher_ed_academic_domain_variant() -> None:
    assert (
        is_excluded_vertical(
            company="Oxford",
            industry="Education",
            naics_text="",
            company_type="",
            domain="ox.ac.uk",
            source_name="contacts.csv",
        )
        is True
    )


def test_excludes_higher_ed_keyword_without_edu_domain() -> None:
    assert (
        is_excluded_vertical(
            company="Some University Press",
            industry="Publishing",
            naics_text="",
            company_type="private company",
            domain="example.com",
            source_name="import.csv",
        )
        is True
    )


def test_excludes_higher_ed_naics_codes() -> None:
    assert (
        is_excluded_vertical(
            company="Acme Learning Group",
            industry="Training",
            naics_text="611310",
            company_type="private company",
            domain="acmelearning.com",
            source_name="import.csv",
        )
        is True
    )


def test_keeps_non_excluded_industrial_contact() -> None:
    assert (
        is_excluded_vertical(
            company="Acme Manufacturing",
            industry="Industrial Machinery",
            naics_text="333999",
            company_type="Privately Held",
            domain="acme.com",
            source_name="manufacturing_contacts.csv",
        )
        is False
    )
