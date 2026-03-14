"""
Pytest configuration for alpha_tracker2. Registers custom marks.
"""

def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "integration: mark test as integration (uses real store when available; may skip)",
    )
