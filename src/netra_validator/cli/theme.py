from rich.theme import Theme

NETRA_CLI_THEME = Theme(
    {
        "brand": "bold #55EAD4",  # Electric Cyan
        "brand_dim": "#04D5DB",  # Darker, non-bold Cyan
        "accent": "bold #F381E5",  # Neon Magenta
        "success": "bold #00FF41",  # Matrix Green
        "success_dim": "#00C72E",  # Darker, non-bold Green
        "warning": "bold #FCEE0C",  # Cyber Yellow
        "critical": "bold #FB0A26",  # Arasaka Red
        "muted": "dim white",  # Ghost White (Structural)
        "white": "white",  # Pure White
        "inactive": "#242424",  # Shadow Grey
    }
)
