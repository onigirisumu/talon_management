STATUS_CHOICES = [
    ("waiting", "Ожидает"),
    ("confirmed", "Подтверждено"),
    ("in_progress", "На приеме"),
    ("completed", "Завершен"),
    ("cancelled", "Отменен"),
    ("no_show", "Не явился"),
]

STATUS_LABELS = dict(STATUS_CHOICES)

ROLE_LABELS = {
    "admin": "Администратор",
    "staff": "Сотрудник",
    "citizen": "Гражданин",
}
