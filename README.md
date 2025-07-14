# real-time-order-book

## Project Structure

- `config/`
  - `__init__.py`
  - `database.py` — Database connection details
  - `exchanges.py` — Exchange API keys and symbols

- `collector/`
  - `__init__.py`
  - `base_collector.py` — Abstract base class for collectors
  - `bitfinex_collector.py` — Specific Bitfinex implementation

- `storage/`
  - `__init__.py`
  - `base_storage.py` — Abstract base class for storage
  - `mysql_storage.py` — MySQL database interaction

- `scheduler/`
  - `__init__.py`
  - `data_scheduler.py` — Handles timing and dispatching collection tasks

- `utils/`
  - `__init__.py`
  - `logger.py` — Centralized logging

- `main.py` — Entry point

- `requirements.txt` — Project dependencies
