__version__ = "0.1"

def get_provider_info():
    return {
        "package-name": "airflow-provider-coalesce",
        "name": "Coalesce",
        "description": "A hook for Airflow and Astronomer to run Coalesce jobs",
        "connection-types": [
            {
                "connection-type": "Coalesce",
                "hook-class-name": "airflow-provider-coalesce.hooks.sample.coalesce_hook"
            }
        ],
        "versions": [__version__]
    }