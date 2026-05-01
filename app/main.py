import os
from datetime import date, datetime, timedelta
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from random import randint
from secrets import token_urlsafe
from urllib.parse import quote
from xml.sax.saxutils import escape

import qrcode
from bson import ObjectId
from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pymongo.errors import DuplicateKeyError
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import Image as PdfImage
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

from .database import (
    accounts,
    appointments,
    ensure_connection,
    ensure_indexes,
    services,
    sessions,
    staff_notes,
    time_slots,
)
from .models import ROLE_LABELS, STATUS_CHOICES, STATUS_LABELS


BASE_DIR = Path(__file__).resolve().parent.parent
SESSION_COOKIE = "appointment_session"
LANG_COOKIE = "appointment_lang"
PASSWORD_SALT = "appointment-practice-project"

SUPPORTED_LANGS = ("rus", "kz", "eng")
LANG_LABELS = {"rus": "RUS", "kz": "KZ", "eng": "ENG"}
SERVICE_CATEGORY_LABELS = {
    "consultations": "Консультации",
    "documents": "Документы",
    "legal_actions": "Юридические действия",
    "notary": "Нотариальные вопросы",
    "digital": "Цифровые услуги",
}
SERVICE_TRANSLATIONS = {
    "Консультация по гражданскому праву": {
        "kz": {
            "name": "Азаматтық құқық бойынша кеңес",
            "description": "Азаматтық-құқықтық мәселелер бойынша құқықтар мен міндеттерді түсіндіру.",
        },
        "eng": {
            "name": "Civil Law Consultation",
            "description": "Explanation of rights and obligations in civil law matters.",
        },
    },
    "Консультация по семейному праву": {
        "kz": {
            "name": "Отбасы құқығы бойынша кеңес",
            "description": "Неке, алимент, қорғаншылық және отбасылық құжаттар бойынша көмек.",
        },
        "eng": {
            "name": "Family Law Consultation",
            "description": "Help with marriage, alimony, guardianship, and family documents.",
        },
    },
    "Консультация по трудовым спорам": {
        "kz": {
            "name": "Еңбек даулары бойынша кеңес",
            "description": "Еңбек қатынастары және өтініштер бойынша бастапқы кеңес.",
        },
        "eng": {
            "name": "Labor Dispute Consultation",
            "description": "Initial consultation on labor relations and appeals.",
        },
    },
    "Консультация по административным вопросам": {
        "kz": {
            "name": "Әкімшілік мәселелер бойынша кеңес",
            "description": "Әкімшілік рәсімдер мен өтініштер тәртібін түсіндіру.",
        },
        "eng": {
            "name": "Administrative Matters Consultation",
            "description": "Explanation of administrative procedures and appeals.",
        },
    },
    "Подача заявления": {
        "kz": {
            "name": "Өтініш беру",
            "description": "Азаматтар мен ұйымдардың өтініштерін қабылдау.",
        },
        "eng": {
            "name": "Application Submission",
            "description": "Receiving applications from citizens and organizations.",
        },
    },
    "Прием документов": {
        "kz": {
            "name": "Құжаттарды қабылдау",
            "description": "Құжаттар пакетін әрі қарай қарау үшін қабылдау.",
        },
        "eng": {
            "name": "Document Intake",
            "description": "Receiving document packages for further review.",
        },
    },
    "Подача документов": {
        "kz": {
            "name": "Құжаттарды тапсыру",
            "description": "Құжаттар пакетін әрі қарай қарау үшін тапсыру.",
        },
        "eng": {
            "name": "Document Submission",
            "description": "Submitting a document package for further review.",
        },
    },
    "Получение справки": {
        "kz": {
            "name": "Анықтама алу",
            "description": "Анықтамалар мен растаушы құжаттарды беру.",
        },
        "eng": {
            "name": "Certificate Issuance",
            "description": "Issuing certificates and supporting documents.",
        },
    },
    "Регистрация обращения": {
        "kz": {
            "name": "Өтінішті тіркеу",
            "description": "Ресми өтінішті нөмір бере отырып тіркеу.",
        },
        "eng": {
            "name": "Appeal Registration",
            "description": "Registering an official appeal with a reference number.",
        },
    },
    "Подача жалобы": {
        "kz": {
            "name": "Шағым беру",
            "description": "Шағымды әрі қарай қарау үшін қабылдау.",
        },
        "eng": {
            "name": "Complaint Submission",
            "description": "Receiving a complaint for further review.",
        },
    },
    "Обжалование решения": {
        "kz": {
            "name": "Шешімге шағымдану",
            "description": "Шешімге шағымдану үшін кеңес және материалдарды қабылдау.",
        },
        "eng": {
            "name": "Decision Appeal",
            "description": "Consultation and intake of materials for appealing a decision.",
        },
    },
    "Подготовка правового обращения": {
        "kz": {
            "name": "Құқықтық өтініш дайындау",
            "description": "Құқықтық өтініш дайындауға көмек.",
        },
        "eng": {
            "name": "Legal Appeal Preparation",
            "description": "Help preparing a legal appeal.",
        },
    },
    "Оформление доверенности": {
        "kz": {
            "name": "Сенімхатты рәсімдеу",
            "description": "Сенімхат рәсімдеу және қажетті құжаттар бойынша кеңес.",
        },
        "eng": {
            "name": "Power of Attorney Preparation",
            "description": "Consultation on preparing a power of attorney and required documents.",
        },
    },
    "Консультация по нотариальным вопросам": {
        "kz": {
            "name": "Нотариат мәселелері бойынша кеңес",
            "description": "Нотариаттық әрекеттер тәртібін түсіндіру.",
        },
        "eng": {
            "name": "Notary Matters Consultation",
            "description": "Explanation of notarial procedures.",
        },
    },
    "Помощь с eGov Kazakhstan": {
        "kz": {
            "name": "eGov Kazakhstan бойынша көмек",
            "description": "Электрондық мемлекеттік қызметтер бойынша кеңес.",
        },
        "eng": {
            "name": "eGov Kazakhstan Assistance",
            "description": "Consultation on electronic government services.",
        },
    },
    "Восстановление доступа": {
        "kz": {
            "name": "Қолжетімділікті қалпына келтіру",
            "description": "Цифрлық сервистерге қолжетімділікті қалпына келтіруге көмек.",
        },
        "eng": {
            "name": "Access Recovery",
            "description": "Help recovering access to digital services.",
        },
    },
    "Консультация по онлайн-заявкам": {
        "kz": {
            "name": "Онлайн-өтінімдер бойынша кеңес",
            "description": "Онлайн-өтінімдерді беру және бақылау бойынша көмек.",
        },
        "eng": {
            "name": "Online Application Consultation",
            "description": "Help submitting and tracking online applications.",
        },
    },
    "Консультация специалиста": {
        "kz": {
            "name": "Маман кеңесі",
            "description": "Қызметтер мен құжаттар бойынша кеңес.",
        },
        "eng": {
            "name": "Specialist Consultation",
            "description": "Consultation on services and documents.",
        },
    },
}
TRANSLATIONS = {
    "rus": {
        "brand": "Запись на прием",
        "nav_services": "Услуги",
        "nav_cabinet": "Личный кабинет",
        "nav_status": "Проверить талон",
        "sign_in": "Войти",
        "logout": "Выйти",
        "hero_title": "Запишитесь на прием без очереди",
        "hero_lead": "Выберите услугу, дату и свободное время. После отправки формы система выдаст номер талона для проверки статуса.",
        "choose_service": "Выберите нужную услугу",
        "book": "Записаться",
        "active_services": "активных услуг",
        "available_slots": "свободных слотов",
        "appointments_today": "записей сегодня",
        "all_services": "Все услуги",
        "consultations": "Консультации",
        "documents": "Документы",
        "legal_actions": "Юридические действия",
        "notary": "Нотариат",
        "digital": "Цифровые услуги",
        "no_services": "Активных услуг пока нет. Добавьте услуги в админ-панели.",
        "back_to_services": "Назад к услугам",
        "appointment_date": "Дата приема",
        "show_time": "Показать время",
        "free_time": "Свободное время",
        "no_slots": "На выбранную дату свободного времени нет.",
        "account_data": "Данные из личного кабинета",
        "full_name": "ФИО",
        "iin": "ИИН",
        "phone": "Телефон",
        "email": "Email",
        "not_specified": "Не указан",
        "comment": "Комментарий",
        "get_ticket": "Получить талон",
        "cancel": "Отмена",
        "next_steps": "Что будет дальше",
        "step_slot": "Выберите свободный слот.",
        "step_account": "Данные гражданина берутся из личного кабинета.",
        "step_ticket": "Получите номер талона.",
        "step_status": "Проверяйте статус записи по номеру.",
        "confirmation_title": "Запись создана",
        "ticket": "Талон",
        "service": "Услуга",
        "date_time": "Дата и время",
        "citizen": "Гражданин",
        "status": "Статус",
        "check_status": "Проверить статус",
        "download_pdf": "Скачать талон PDF",
        "new_appointment": "Новая запись",
        "check_ticket_title": "Проверить номер талона",
        "ticket_number": "Номер талона",
        "check": "Проверить",
        "details": "Детали записи",
        "ticket_not_found": "Талон с таким номером не найден.",
        "enter_ticket": "Введите номер талона, чтобы увидеть текущий статус записи.",
        "client_notice_title": "Уважаемые клиенты!",
        "client_notice_intro": "Если Вы не смогли забронировать подходящее для Вас время, Вы можете посетить отделение банка без онлайн-бронирования очереди.",
        "client_notice_booking": "В случае бронирования, Вам необходимо:",
        "client_notice_arrive": "Прибыть за 15 минут до наступления забронированного времени;",
        "client_notice_document": "Иметь при себе оригинал документа, удостоверяющего личность.",
        "cabinet_appointments": "Мои записи",
        "no_appointments": "У вас пока нет записей.",
        "reschedule": "Перенести",
        "reschedule_title": "Перенос записи",
        "choose_new_time": "Выберите новое время",
        "save_reschedule": "Сохранить перенос",
        "login_title": "Вход в аккаунт",
        "identifier": "Телефон, email или логин",
        "password": "Пароль",
        "confirm_password": "Подтвердите пароль",
        "no_account": "Нет аккаунта?",
        "signup": "Зарегистрироваться",
        "signup_title": "Создать кабинет",
        "register_method": "Способ регистрации",
        "already_account": "Уже есть аккаунт?",
        "category": "Категория",
    },
    "kz": {
        "brand": "Қабылдауға жазылу",
        "nav_services": "Қызметтер",
        "nav_cabinet": "Жеке кабинет",
        "nav_status": "Талонды тексеру",
        "sign_in": "Кіру",
        "logout": "Шығу",
        "hero_title": "Кезексіз қабылдауға жазылыңыз",
        "hero_lead": "Қызметті, күнді және бос уақытты таңдаңыз. Өтінімнен кейін жүйе талон нөмірін береді.",
        "choose_service": "Қажетті қызметті таңдаңыз",
        "book": "Жазылу",
        "active_services": "белсенді қызмет",
        "available_slots": "бос уақыт",
        "appointments_today": "бүгінгі жазба",
        "all_services": "Барлық қызметтер",
        "consultations": "Кеңестер",
        "documents": "Құжаттар",
        "legal_actions": "Құқықтық әрекеттер",
        "notary": "Нотариат",
        "digital": "Цифрлық қызметтер",
        "no_services": "Белсенді қызметтер жоқ.",
        "back_to_services": "Қызметтерге оралу",
        "appointment_date": "Қабылдау күні",
        "show_time": "Уақытты көрсету",
        "free_time": "Бос уақыт",
        "no_slots": "Таңдалған күнге бос уақыт жоқ.",
        "account_data": "Жеке кабинет деректері",
        "full_name": "Аты-жөні",
        "iin": "ЖСН",
        "phone": "Телефон",
        "email": "Email",
        "not_specified": "Көрсетілмеген",
        "comment": "Пікір",
        "get_ticket": "Талон алу",
        "cancel": "Бас тарту",
        "next_steps": "Әрі қарай",
        "step_slot": "Бос уақытты таңдаңыз.",
        "step_account": "Азамат деректері жеке кабинеттен алынады.",
        "step_ticket": "Талон нөмірін алыңыз.",
        "step_status": "Жазба мәртебесін талон нөмірі арқылы тексеріңіз.",
        "confirmation_title": "Жазба жасалды",
        "ticket": "Талон",
        "service": "Қызмет",
        "date_time": "Күні мен уақыты",
        "citizen": "Азамат",
        "status": "Мәртебе",
        "check_status": "Мәртебені тексеру",
        "download_pdf": "PDF талонды жүктеу",
        "new_appointment": "Жаңа жазба",
        "check_ticket_title": "Талон нөмірін тексеру",
        "ticket_number": "Талон нөмірі",
        "check": "Тексеру",
        "details": "Жазба мәліметтері",
        "ticket_not_found": "Мұндай талон табылмады.",
        "enter_ticket": "Жазба мәртебесін көру үшін талон нөмірін енгізіңіз.",
        "client_notice_title": "Құрметті клиенттер!",
        "client_notice_intro": "Егер өзіңізге қолайлы уақытты брондай алмасаңыз, бөлімшеге онлайн брондаусыз келе аласыз.",
        "client_notice_booking": "Брондау жасалған жағдайда:",
        "client_notice_arrive": "Брондалған уақыттан 15 минут бұрын келіңіз;",
        "client_notice_document": "Жеке басты куәландыратын құжаттың түпнұсқасын өзіңізбен бірге алыңыз.",
        "cabinet_appointments": "Менің жазбаларым",
        "no_appointments": "Сізде әзірге жазбалар жоқ.",
        "reschedule": "Ауыстыру",
        "reschedule_title": "Жазбаны ауыстыру",
        "choose_new_time": "Жаңа уақытты таңдаңыз",
        "save_reschedule": "Ауыстыруды сақтау",
        "login_title": "Аккаунтқа кіру",
        "identifier": "Телефон, email немесе логин",
        "password": "Құпиясөз",
        "confirm_password": "Құпиясөзді растаңыз",
        "no_account": "Аккаунтыңыз жоқ па?",
        "signup": "Тіркелу",
        "signup_title": "Кабинет ашу",
        "register_method": "Тіркелу тәсілі",
        "already_account": "Аккаунтыңыз бар ма?",
        "category": "Санат",
    },
    "eng": {
        "brand": "Appointment Booking",
        "nav_services": "Services",
        "nav_cabinet": "Cabinet",
        "nav_status": "Check ticket",
        "sign_in": "Sign in",
        "logout": "Sign out",
        "hero_title": "Book an appointment without waiting in line",
        "hero_lead": "Choose a service, date, and available time. After booking, the system will issue a ticket number.",
        "choose_service": "Choose a service",
        "book": "Book",
        "active_services": "active services",
        "available_slots": "available slots",
        "appointments_today": "appointments today",
        "all_services": "All services",
        "consultations": "Consultations",
        "documents": "Documents",
        "legal_actions": "Legal actions",
        "notary": "Notary",
        "digital": "Digital services",
        "no_services": "No active services yet.",
        "back_to_services": "Back to services",
        "appointment_date": "Appointment date",
        "show_time": "Show time",
        "free_time": "Available time",
        "no_slots": "No available time for the selected date.",
        "account_data": "Cabinet data",
        "full_name": "Full name",
        "iin": "IIN",
        "phone": "Phone",
        "email": "Email",
        "not_specified": "Not specified",
        "comment": "Comment",
        "get_ticket": "Get ticket",
        "cancel": "Cancel",
        "next_steps": "What happens next",
        "step_slot": "Choose an available slot.",
        "step_account": "Citizen data is taken from the cabinet.",
        "step_ticket": "Get your ticket number.",
        "step_status": "Check appointment status by ticket number.",
        "confirmation_title": "Appointment created",
        "ticket": "Ticket",
        "service": "Service",
        "date_time": "Date and time",
        "citizen": "Citizen",
        "status": "Status",
        "check_status": "Check status",
        "download_pdf": "Download PDF ticket",
        "new_appointment": "New appointment",
        "check_ticket_title": "Check ticket number",
        "ticket_number": "Ticket number",
        "check": "Check",
        "details": "Appointment details",
        "ticket_not_found": "No ticket with this number was found.",
        "enter_ticket": "Enter a ticket number to see the current appointment status.",
        "client_notice_title": "Dear clients!",
        "client_notice_intro": "If you could not book a suitable time, you may visit the office without online queue booking.",
        "client_notice_booking": "If you have booked an appointment, please:",
        "client_notice_arrive": "Arrive 15 minutes before the booked time;",
        "client_notice_document": "Bring the original identity document with you.",
        "cabinet_appointments": "My appointments",
        "no_appointments": "You do not have appointments yet.",
        "reschedule": "Reschedule",
        "reschedule_title": "Reschedule appointment",
        "choose_new_time": "Choose a new time",
        "save_reschedule": "Save reschedule",
        "login_title": "Sign in",
        "identifier": "Phone, email, or username",
        "password": "Password",
        "confirm_password": "Confirm password",
        "no_account": "Do not have an account?",
        "signup": "Sign up",
        "signup_title": "Create account",
        "register_method": "Registration method",
        "already_account": "Already have an account?",
        "category": "Category",
    },
}

templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
templates.env.filters["date_ru"] = lambda value: format_date(value)

app = FastAPI(title="Запись на прием")
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


@app.on_event("startup")
def startup():
    ensure_connection()
    ensure_indexes()
    seed_demo_data()


def normalize(value: str) -> str:
    return value.strip()


def normalize_phone(value: str) -> str:
    return "".join(char for char in value.strip() if char.isdigit() or char == "+")


def normalize_email(value: str) -> str:
    return value.strip().lower()


def normalize_login_identifier(value: str) -> str:
    value = value.strip()
    if "@" in value:
        return normalize_email(value)
    if any(char.isdigit() for char in value):
        return normalize_phone(value)
    return value


def hash_password(password: str) -> str:
    return sha256(f"{PASSWORD_SALT}:{password}".encode("utf-8")).hexdigest()


def verify_password(password: str, password_hash: str) -> bool:
    return hash_password(password) == password_hash


def email_is_valid(value: str) -> bool:
    local, separator, domain = value.partition("@")
    return bool(local and separator and "." in domain and not domain.startswith(".") and not domain.endswith("."))


def phone_is_valid(value: str) -> bool:
    return value.startswith("+7") and value[1:].isdigit()


def password_rule_error(password: str) -> str:
    if len(password) < 8:
        return "Пароль должен содержать минимум 8 символов."
    if not any(char.isdigit() for char in password):
        return "Пароль должен содержать минимум одну цифру."
    if not any(not char.isalnum() for char in password):
        return "Пароль должен содержать минимум один специальный знак."
    return ""


def now_utc():
    return datetime.utcnow()


def current_path(request: Request) -> str:
    path = request.url.path
    if request.url.query:
        path = f"{path}?{request.url.query}"
    return path


def safe_next_url(value: str) -> str:
    if value and value.startswith("/") and not value.startswith("//"):
        return value
    return "/"


def get_lang(request: Request) -> str:
    lang = request.cookies.get(LANG_COOKIE, "rus")
    return lang if lang in SUPPORTED_LANGS else "rus"


def lang_links(request: Request, current_lang: str):
    next_url = quote(current_path(request))
    return [
        {
            "code": code,
            "label": LANG_LABELS[code],
            "active": code == current_lang,
            "href": f"/language/{code}?next={next_url}",
        }
        for code in SUPPORTED_LANGS
    ]


def oid(value: str) -> ObjectId:
    try:
        return ObjectId(value)
    except Exception as exc:
        raise HTTPException(status_code=404, detail="Запись не найдена") from exc


def format_date(value) -> str:
    if not value:
        return ""
    if isinstance(value, date):
        return value.strftime("%d.%m.%Y")
    try:
        return date.fromisoformat(str(value)).strftime("%d.%m.%Y")
    except ValueError:
        return str(value)


def doc_id(document: dict) -> str:
    return str(document["_id"])


def clean_account(account: dict | None) -> dict | None:
    if not account:
        return None
    data = dict(account)
    data["id"] = doc_id(data)
    data["role_label"] = ROLE_LABELS.get(data.get("role"), data.get("role", ""))
    data.pop("password_hash", None)
    return data


def clean_service(service: dict | None, lang: str = "rus") -> dict | None:
    if not service:
        return None
    data = dict(service)
    data["id"] = doc_id(data)
    data["category"] = data.get("category", "documents")
    data["category_label"] = TRANSLATIONS.get(lang, TRANSLATIONS["rus"]).get(
        data["category"],
        SERVICE_CATEGORY_LABELS.get(data["category"], data["category"]),
    )
    service_translation = SERVICE_TRANSLATIONS.get(data.get("name", ""), {}).get(lang)
    if service_translation:
        data["name"] = service_translation["name"]
        data["description"] = service_translation["description"]
    return data


def clean_slot(slot: dict | None) -> dict | None:
    if not slot:
        return None
    data = dict(slot)
    data["id"] = doc_id(data)
    data["date_display"] = format_date(data.get("date"))
    return data


def clean_appointment(appointment: dict | None, lang: str = "rus") -> dict | None:
    if not appointment:
        return None
    data = dict(appointment)
    data["id"] = doc_id(data)
    data["service"] = clean_service(services.find_one({"_id": data["service_id"]}), lang)
    data["time_slot"] = clean_slot(time_slots.find_one({"_id": data["time_slot_id"]}))
    data["citizen"] = clean_account(accounts.find_one({"_id": data["citizen_id"]}))
    data["date_display"] = format_date(data.get("date"))
    data["status_label"] = STATUS_LABELS.get(data.get("status"), data.get("status", ""))
    return data


def get_current_user(request: Request) -> dict | None:
    token = request.cookies.get(SESSION_COOKIE)
    if not token:
        return None
    session = sessions.find_one({"token": token})
    if not session:
        return None
    return clean_account(accounts.find_one({"_id": session["account_id"]}))


def create_session_response(account: dict, redirect_url: str) -> RedirectResponse:
    token = token_urlsafe(32)
    sessions.insert_one({"token": token, "account_id": account["_id"], "created_at": now_utc()})
    response = RedirectResponse(url=redirect_url, status_code=303)
    response.set_cookie(
        SESSION_COOKIE,
        token,
        httponly=True,
        samesite="lax",
        max_age=60 * 60 * 24 * 30,
    )
    return response


def clear_session_response(request: Request, redirect_url: str = "/") -> RedirectResponse:
    token = request.cookies.get(SESSION_COOKIE)
    if token:
        sessions.delete_one({"token": token})
    response = RedirectResponse(url=redirect_url, status_code=303)
    response.delete_cookie(SESSION_COOKIE)
    return response


def role_home(role: str) -> str:
    return {
        "admin": "/admin",
        "staff": "/staff",
        "citizen": "/cabinet",
    }.get(role, "/")


def next_url_for_role(next_url: str, role: str) -> str:
    if not next_url or not next_url.startswith("/") or next_url.startswith("//"):
        return role_home(role)

    allowed_paths = {
        "admin": ["/admin"],
        "staff": ["/staff"],
        "citizen": ["/cabinet", "/book", "/confirmation"],
    }.get(role, [role_home(role)])
    for path in allowed_paths:
        if next_url == path or next_url.startswith(f"{path}/") or next_url.startswith(f"{path}?"):
            return next_url
    return role_home(role)


def role_redirect(request: Request, role: str) -> RedirectResponse:
    return RedirectResponse(url=f"/login?next={quote(current_path(request))}", status_code=303)


def require_role(request: Request, role: str):
    user = get_current_user(request)
    if not user:
        return None, role_redirect(request, role)
    if user.get("role") != role:
        return None, RedirectResponse(url=role_home(user.get("role")), status_code=303)
    return user, None


def render(request: Request, template_name: str, context: dict):
    lang = get_lang(request)
    context.update(
        {
            "request": request,
            "current_user": get_current_user(request),
            "current_lang": lang,
            "lang_links": lang_links(request, lang),
            "t": TRANSLATIONS[lang],
            "status_choices": STATUS_CHOICES,
            "status_labels": STATUS_LABELS,
            "role_labels": ROLE_LABELS,
            "today": date.today().isoformat(),
        }
    )
    return templates.TemplateResponse(request, template_name, context)


@app.get("/language/{lang}")
def switch_language(lang: str, next: str = "/"):
    if lang not in SUPPORTED_LANGS:
        lang = "rus"
    response = RedirectResponse(url=safe_next_url(next), status_code=303)
    response.set_cookie(LANG_COOKIE, lang, httponly=False, samesite="lax", max_age=60 * 60 * 24 * 365)
    return response


def upsert_account(username: str, password: str, role: str, full_name: str):
    existing = accounts.find_one({"username": username})
    data = {
        "username": username,
        "password_hash": hash_password(password),
        "role": role,
        "full_name": full_name,
        "created_at": now_utc(),
    }
    if existing:
        accounts.update_one({"_id": existing["_id"]}, {"$set": data})
    else:
        accounts.insert_one(data)


def find_citizen_by_identifier(identifier: str) -> dict | None:
    normalized = normalize_login_identifier(identifier)
    if not normalized:
        return None
    if "@" in normalized:
        return accounts.find_one({"role": "citizen", "email": normalized})
    return accounts.find_one({"role": "citizen", "phone": normalized})


def render_login(
    request: Request,
    next_url: str = "",
    error: str = "",
    register_error: str = "",
    register_message: str = "",
    form_data: dict | None = None,
    mode: str = "signin",
):
    auth_mode = "signup" if mode == "signup" else "signin"
    return render(
        request,
        "login.html",
        {
            "next_url": next_url or "",
            "next_param": quote(next_url or ""),
            "auth_mode": auth_mode,
            "error": error,
            "register_error": register_error,
            "register_message": register_message,
            "form_data": form_data or {},
        },
    )


def seed_demo_data():
    upsert_account("admin", "Admin123!", "admin", "Администратор")
    upsert_account("staff", "Staff123!", "staff", "Сотрудник")

    demo_services = [
        ("Консультация по гражданскому праву", "Разъяснение прав и обязанностей по гражданско-правовым вопросам.", 30, "consultations"),
        ("Консультация по семейному праву", "Помощь по вопросам брака, алиментов, опеки и семейных документов.", 30, "consultations"),
        ("Консультация по трудовым спорам", "Первичная консультация по трудовым отношениям и обращениям.", 30, "consultations"),
        ("Консультация по административным вопросам", "Разъяснение порядка административных процедур и обращений.", 30, "consultations"),
        ("Подача заявления", "Прием заявлений от граждан и организаций.", 30, "documents"),
        ("Прием документов", "Прием пакета документов для дальнейшего рассмотрения.", 40, "documents"),
        ("Получение справки", "Выдача справок и подтверждающих документов.", 20, "documents"),
        ("Регистрация обращения", "Регистрация официального обращения с выдачей номера.", 20, "documents"),
        ("Подача жалобы", "Прием жалобы для дальнейшего рассмотрения.", 30, "legal_actions"),
        ("Обжалование решения", "Консультация и прием материалов для обжалования решения.", 40, "legal_actions"),
        ("Подготовка правового обращения", "Помощь в подготовке правового обращения.", 40, "legal_actions"),
        ("Оформление доверенности", "Консультация по оформлению доверенности и необходимым документам.", 30, "notary"),
        ("Консультация по нотариальным вопросам", "Разъяснение порядка нотариальных действий.", 30, "notary"),
        ("Помощь с eGov Kazakhstan", "Консультация по электронным государственным услугам.", 25, "digital"),
        ("Восстановление доступа", "Помощь с восстановлением доступа к цифровым сервисам.", 25, "digital"),
        ("Консультация по онлайн-заявкам", "Помощь при подаче и отслеживании онлайн-заявок.", 25, "digital"),
    ]
    for name, description, duration, category in demo_services:
        existing = services.find_one({"name": name})
        data = {
            "name": name,
            "description": description,
            "duration": duration,
            "category": category,
            "is_active": True,
        }
        if existing:
            services.update_one({"_id": existing["_id"]}, {"$set": {"category": category}})
        else:
            services.insert_one(data)

    if time_slots.count_documents({}) == 0:
        slots = []
        for offset in range(1, 11):
            slot_date = date.today() + timedelta(days=offset)
            if slot_date.weekday() >= 5:
                continue
            for hour in range(9, 17):
                slots.append(
                    {
                        "date": slot_date.isoformat(),
                        "start_time": f"{hour:02d}:00",
                        "end_time": f"{hour:02d}:30",
                        "is_available": True,
                    }
                )
        if slots:
            time_slots.insert_many(slots)


def get_service_or_404(service_id: str, lang: str = "rus") -> dict:
    service = services.find_one({"_id": oid(service_id)})
    if service is None:
        raise HTTPException(status_code=404, detail="Услуга не найдена")
    return clean_service(service, lang)


def available_dates():
    rows = time_slots.distinct(
        "date",
        {"date": {"$gte": date.today().isoformat()}, "is_available": True},
    )
    return sorted(rows)


def generate_ticket_number():
    prefix = f"T-{date.today():%Y%m%d}"
    while True:
        ticket_number = f"{prefix}-{randint(1000, 9999)}"
        if not appointments.find_one({"ticket_number": ticket_number}):
            return ticket_number


def get_daily_appointments(selected_date: str, lang: str = "rus"):
    rows = appointments.find({"date": selected_date}).sort([("date", 1), ("created_at", 1)])
    return sorted(
        [clean_appointment(row, lang) for row in rows],
        key=lambda item: item["time_slot"]["start_time"] if item.get("time_slot") else "",
    )


def get_citizen_appointment_or_404(appointment_id: str, user: dict) -> dict:
    appointment = appointments.find_one({"_id": oid(appointment_id), "citizen_id": ObjectId(user["id"])})
    if appointment is None:
        raise HTTPException(status_code=404, detail="Запись не найдена")
    return appointment


def pdf_font_name() -> str:
    if "TicketFont" in pdfmetrics.getRegisteredFontNames():
        return "TicketFont"
    win_fonts = Path(os.environ.get("SystemRoot", "C:/Windows")) / "Fonts"
    candidates = [
        str(win_fonts / "arial.ttf"),
        str(win_fonts / "Arial.ttf"),
        str(win_fonts / "times.ttf"),
        "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
        "/System/Library/Fonts/Supplemental/Arial.ttf",
        "/Library/Fonts/Arial.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ]
    for font_path in candidates:
        if Path(font_path).exists():
            pdfmetrics.registerFont(TTFont("TicketFont", font_path))
            print(f"[PDF] Registered font: {font_path}")
            return "TicketFont"
    print("[PDF] WARNING: No Cyrillic font found, falling back to Helvetica")
    return "Helvetica"


def build_ticket_pdf(request: Request, appointment: dict) -> BytesIO:
    status_url = str(request.url_for("status_page")) + f"?ticket={quote(appointment['ticket_number'])}"
    qr_image = qrcode.make(status_url)
    qr_buffer = BytesIO()
    qr_image.save(qr_buffer, format="PNG")
    qr_buffer.seek(0)

    pdf_buffer = BytesIO()
    font = pdf_font_name()
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="TicketTitle", fontName=font, fontSize=22, leading=28, spaceAfter=14))
    styles.add(ParagraphStyle(name="TicketText", fontName=font, fontSize=12, leading=17))
    doc = SimpleDocTemplate(
        pdf_buffer,
        pagesize=A4,
        rightMargin=20 * mm,
        leftMargin=20 * mm,
        topMargin=18 * mm,
        bottomMargin=18 * mm,
    )
    rows = [
        ["Номер талона", appointment["ticket_number"]],
        ["ФИО", appointment.get("full_name", "")],
        ["Услуга", appointment["service"]["name"]],
        ["Дата", format_date(appointment.get("date"))],
        ["Время", appointment["time_slot"]["start_time"]],
        ["Статус", appointment["status_label"]],
    ]
    table = Table([[Paragraph(escape(str(left)), styles["TicketText"]), Paragraph(escape(str(right)), styles["TicketText"])] for left, right in rows], colWidths=[42 * mm, 110 * mm])
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#eaf3ff")),
                ("BOX", (0, 0), (-1, -1), 0.7, colors.HexColor("#bfd8ff")),
                ("INNERGRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#bfd8ff")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 7),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
            ]
        )
    )
    notice = (
        "Уважаемые клиенты! Если Вы не смогли забронировать подходящее для Вас время, "
        "Вы можете посетить отделение банка без онлайн-бронирования очереди. "
        "При бронировании прибудьте за 15 минут до времени и возьмите оригинал документа, "
        "удостоверяющего личность."
    )
    story = [
        Paragraph("Талон на прием", styles["TicketTitle"]),
        table,
        Spacer(1, 14),
        PdfImage(qr_buffer, width=38 * mm, height=38 * mm),
        Spacer(1, 10),
        Paragraph(escape(status_url), styles["TicketText"]),
        Spacer(1, 16),
        Paragraph(escape(notice), styles["TicketText"]),
    ]
    doc.build(story)
    pdf_buffer.seek(0)
    return pdf_buffer


@app.get("/")
def home(request: Request, category: str = ""):
    lang = get_lang(request)
    service_query = {"is_active": True}
    if category in SERVICE_CATEGORY_LABELS:
        service_query["category"] = category
    active_services = [
        clean_service(item, lang)
        for item in services.find(service_query).sort("name", 1)
    ]
    appointments_today = appointments.count_documents({"date": date.today().isoformat()})
    available_slots = time_slots.count_documents(
        {"date": {"$gte": date.today().isoformat()}, "is_available": True}
    )
    return render(
        request,
        "home.html",
        {
            "services": active_services,
            "selected_category": category if category in SERVICE_CATEGORY_LABELS else "",
            "service_categories": list(SERVICE_CATEGORY_LABELS.keys()),
            "services_count": len(active_services),
            "appointments_today": appointments_today,
            "available_slots": available_slots,
        },
    )


@app.get("/services")
def service_list(request: Request):
    return home(request)


@app.get("/login")
def login_page(
    request: Request,
    next: str = "",
    error: str = "",
    mode: str = "signin",
):
    return render_login(request, next_url=next, error=error, mode=mode)


@app.post("/login")
def login_submit(
    request: Request,
    identifier: str = Form(...),
    password: str = Form(""),
    next_url: str = Form(""),
):
    identifier = normalize(identifier)
    password = password.strip()

    service_account = accounts.find_one(
        {"username": identifier, "role": {"$in": ["admin", "staff"]}}
    )
    if service_account:
        if not password or not verify_password(password, service_account.get("password_hash", "")):
            return render_login(
                request,
                next_url=next_url,
                error="Неверный логин или пароль.",
                form_data={"identifier": identifier},
            )
        return create_session_response(
            service_account,
            next_url_for_role(next_url, service_account["role"]),
        )

    citizen_account = find_citizen_by_identifier(identifier)
    if citizen_account:
        if not verify_password(password, citizen_account.get("password_hash", "")):
            return render_login(
                request,
                next_url=next_url,
                error="Неверный логин или пароль.",
                form_data={"identifier": identifier},
            )
        return create_session_response(citizen_account, next_url_for_role(next_url, "citizen"))

    return render_login(
        request,
        next_url=next_url,
        error="Аккаунт не найден. Проверьте телефон, email или логин.",
        form_data={"identifier": identifier},
    )


@app.get("/citizen/login")
def citizen_login_page(request: Request, next: str = "", error: str = ""):
    url = f"/login?next={quote(next)}" if next else "/login"
    if error:
        separator = "&" if next else "?"
        url = f"{url}{separator}error={quote(error)}"
    return RedirectResponse(url=url, status_code=303)


@app.post("/citizen/login")
def citizen_login_submit(
    request: Request,
    iin: str = Form(...),
    phone: str = Form(...),
    email: str = Form(...),
    full_name: str = Form(""),
    next_url: str = Form(""),
):
    return register_citizen(
        request,
        register_method="phone",
        full_name=full_name,
        contact=phone,
        iin=iin,
        next_url=next_url,
    )


@app.post("/register")
def register_citizen(
    request: Request,
    register_method: str = Form("phone"),
    full_name: str = Form(""),
    contact: str = Form(""),
    iin: str = Form(""),
    password: str = Form(""),
    confirm_password: str = Form(""),
    next_url: str = Form(""),
):
    register_method = normalize(register_method)
    full_name = normalize(full_name)
    contact = normalize(contact)
    iin = normalize(iin)
    form_data = {
        "register_method": register_method,
        "full_name": full_name,
        "contact": contact,
        "iin": iin,
    }

    account_data = {
        "role": "citizen",
        "full_name": full_name,
        "password_hash": hash_password(password),
        "created_at": now_utc(),
    }
    if iin:
        account_data["iin"] = iin

    pwd_error = password_rule_error(password)
    if register_method not in {"phone", "email"}:
        error = "Выберите регистрацию через телефон или email."
    elif not full_name:
        error = "Введите ФИО."
    elif pwd_error:
        error = pwd_error
    elif password != confirm_password:
        error = "Пароли не совпадают."
    elif iin and (not iin.isdigit() or len(iin) != 12):
        error = "ИИН должен содержать ровно 12 цифр."
    elif register_method == "phone":
        phone = normalize_phone(contact)
        form_data["contact"] = phone
        if not phone:
            error = "Введите телефон."
        elif not phone_is_valid(phone):
            error = "Телефон должен начинаться с +7 и содержать только цифры после плюса."
        elif accounts.find_one({"role": "citizen", "phone": phone}):
            error = "Этот телефон уже зарегистрирован."
        else:
            account_data["phone"] = phone
            error = ""
    else:
        email = normalize_email(contact)
        form_data["contact"] = email
        if not email_is_valid(email):
            error = "Введите email с доменом, например name@gmail.com."
        elif accounts.find_one({"role": "citizen", "email": email}):
            error = "Этот email уже зарегистрирован."
        else:
            account_data["email"] = email
            error = ""

    if not error and iin and accounts.find_one({"role": "citizen", "iin": iin}):
        error = "Этот ИИН уже зарегистрирован."

    if error:
        return render_login(
            request,
            next_url=next_url,
            register_error=error,
            form_data=form_data,
            mode="signup",
        )

    try:
        result = accounts.insert_one(account_data)
    except DuplicateKeyError:
        return render_login(
            request,
            next_url=next_url,
            register_error="Такой аккаунт уже существует.",
            form_data=form_data,
            mode="signup",
        )

    account = accounts.find_one({"_id": result.inserted_id})
    return create_session_response(account, next_url_for_role(next_url, "citizen"))


@app.post("/logout")
def logout(request: Request):
    return clear_session_response(request)


@app.get("/cabinet")
def cabinet(request: Request):
    user, redirect = require_role(request, "citizen")
    if redirect:
        return redirect
    lang = get_lang(request)
    rows = appointments.find({"citizen_id": ObjectId(user["id"])}).sort("created_at", -1)
    return render(
        request,
        "cabinet.html",
        {"appointments": [clean_appointment(row, lang) for row in rows]},
    )


@app.get("/book/{service_id}")
def book_form(request: Request, service_id: str, selected_date: str | None = None):
    user, redirect = require_role(request, "citizen")
    if redirect:
        return redirect

    service = get_service_or_404(service_id, get_lang(request))
    dates = available_dates()
    current_date = selected_date if selected_date in dates else (dates[0] if dates else date.today().isoformat())
    slots = [
        clean_slot(item)
        for item in time_slots.find({"date": current_date, "is_available": True}).sort("start_time", 1)
    ]
    return render(
        request,
        "book.html",
        {
            "service": service,
            "citizen": user,
            "dates": dates,
            "selected_date": current_date,
            "slots": slots,
            "error": "",
        },
    )


@app.post("/book/{service_id}")
def create_appointment(
    request: Request,
    service_id: str,
    appointment_date: str = Form(...),
    time_slot_id: str = Form(...),
    comment: str = Form(""),
):
    user, redirect = require_role(request, "citizen")
    if redirect:
        return redirect

    service = get_service_or_404(service_id, get_lang(request))
    slot = time_slots.find_one({"_id": oid(time_slot_id)})

    if not appointment_date:
        error = "Выберите корректную дату."
    elif slot is None or slot.get("date") != appointment_date or not slot.get("is_available"):
        error = "Выбранное время уже недоступно. Пожалуйста, выберите другой слот."
    else:
        ticket_number = generate_ticket_number()
        appointment = {
            "ticket_number": ticket_number,
            "service_id": ObjectId(service["id"]),
            "citizen_id": ObjectId(user["id"]),
            "full_name": user["full_name"],
            "iin": user.get("iin", ""),
            "phone": user.get("phone", ""),
            "email": user.get("email", ""),
            "date": appointment_date,
            "time_slot_id": slot["_id"],
            "status": "waiting",
            "comment": normalize(comment),
            "created_at": now_utc(),
        }
        appointments.insert_one(appointment)
        time_slots.update_one({"_id": slot["_id"]}, {"$set": {"is_available": False}})
        return RedirectResponse(url=f"/confirmation/{ticket_number}", status_code=303)

    dates = available_dates()
    slots = [
        clean_slot(item)
        for item in time_slots.find({"date": appointment_date, "is_available": True}).sort("start_time", 1)
    ]
    return render(
        request,
        "book.html",
        {
            "service": service,
            "citizen": user,
            "dates": dates,
            "selected_date": appointment_date,
            "slots": slots,
            "error": error,
        },
    )


@app.get("/confirmation/{ticket_number}")
def confirmation(ticket_number: str, request: Request):
    appointment = clean_appointment(appointments.find_one({"ticket_number": ticket_number}), get_lang(request))
    if appointment is None:
        raise HTTPException(status_code=404, detail="Запись не найдена")
    return render(request, "confirmation.html", {"appointment": appointment})


@app.get("/ticket/{ticket_number}", name="ticket_pdf")
def ticket_pdf(ticket_number: str, request: Request):
    appointment = clean_appointment(appointments.find_one({"ticket_number": ticket_number}), get_lang(request))
    if appointment is None:
        raise HTTPException(status_code=404, detail="Запись не найдена")
    pdf_buffer = build_ticket_pdf(request, appointment)
    filename = f"ticket-{ticket_number}.pdf"
    return StreamingResponse(
        pdf_buffer,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/status")
def status_page(request: Request, ticket: str | None = None):
    appointment = None
    if ticket:
        appointment = clean_appointment(appointments.find_one({"ticket_number": ticket.strip()}), get_lang(request))
    return render(
        request,
        "status.html",
        {
            "ticket": ticket or "",
            "appointment": appointment,
            "not_found": bool(ticket and appointment is None),
        },
    )


@app.post("/status")
def check_status(ticket_number: str = Form(...)):
    return RedirectResponse(url=f"/status?ticket={quote(ticket_number.strip())}", status_code=303)


@app.get("/appointments/{appointment_id}/reschedule")
def reschedule_page(request: Request, appointment_id: str, selected_date: str | None = None, error: str = ""):
    user, redirect = require_role(request, "citizen")
    if redirect:
        return redirect
    appointment = clean_appointment(get_citizen_appointment_or_404(appointment_id, user), get_lang(request))
    dates = available_dates()
    current_date = selected_date if selected_date in dates else (dates[0] if dates else date.today().isoformat())
    slots = [
        clean_slot(item)
        for item in time_slots.find({"date": current_date, "is_available": True}).sort("start_time", 1)
    ]
    return render(
        request,
        "reschedule.html",
        {
            "appointment": appointment,
            "dates": dates,
            "selected_date": current_date,
            "slots": slots,
            "error": error,
        },
    )


@app.post("/appointments/{appointment_id}/reschedule")
def reschedule_submit(
    request: Request,
    appointment_id: str,
    appointment_date: str = Form(...),
    time_slot_id: str = Form(...),
):
    user, redirect = require_role(request, "citizen")
    if redirect:
        return redirect
    appointment = get_citizen_appointment_or_404(appointment_id, user)
    if appointment.get("status") in {"completed", "cancelled", "no_show"}:
        return RedirectResponse(
            url=f"/appointments/{appointment_id}/reschedule?error={quote('Эту запись уже нельзя перенести.')}",
            status_code=303,
        )

    new_slot = time_slots.find_one({"_id": oid(time_slot_id)})
    if new_slot is None or new_slot.get("date") != appointment_date or not new_slot.get("is_available"):
        return RedirectResponse(
            url=f"/appointments/{appointment_id}/reschedule?selected_date={quote(appointment_date)}&error={quote('Выбранное время недоступно.')}",
            status_code=303,
        )

    old_slot_id = appointment.get("time_slot_id")
    if old_slot_id:
        time_slots.update_one({"_id": old_slot_id}, {"$set": {"is_available": True}})
    time_slots.update_one({"_id": new_slot["_id"]}, {"$set": {"is_available": False}})
    appointments.update_one(
        {"_id": appointment["_id"]},
        {
            "$set": {
                "date": appointment_date,
                "time_slot_id": new_slot["_id"],
                "status": "waiting",
                "rescheduled_at": now_utc(),
            }
        },
    )
    return RedirectResponse(url=f"/confirmation/{appointment['ticket_number']}", status_code=303)


@app.get("/staff")
def staff_panel(request: Request, selected_date: str | None = None):
    user, redirect = require_role(request, "staff")
    if redirect:
        return redirect
    current_date = selected_date or date.today().isoformat()
    return render(
        request,
        "staff.html",
        {
            "appointments": get_daily_appointments(current_date, get_lang(request)),
            "selected_date": current_date,
            "staff_user": user,
        },
    )


@app.post("/staff/{appointment_id}/status")
def update_staff_status(
    request: Request,
    appointment_id: str,
    selected_date: str = Form(...),
    status: str = Form(...),
    note: str = Form(""),
):
    user, redirect = require_role(request, "staff")
    if redirect:
        return redirect
    if status not in STATUS_LABELS:
        raise HTTPException(status_code=400, detail="Некорректный статус")

    appointment = appointments.find_one({"_id": oid(appointment_id)})
    if appointment is None:
        raise HTTPException(status_code=404, detail="Запись не найдена")

    appointments.update_one({"_id": appointment["_id"]}, {"$set": {"status": status}})
    if appointment.get("time_slot_id"):
        time_slots.update_one(
            {"_id": appointment["time_slot_id"]},
            {"$set": {"is_available": status == "cancelled"}},
        )
    if note.strip():
        staff_notes.insert_one(
            {
                "appointment_id": appointment["_id"],
                "staff_id": ObjectId(user["id"]),
                "staff": user["full_name"],
                "note": normalize(note),
                "created_at": now_utc(),
            }
        )
    return RedirectResponse(url=f"/staff?selected_date={quote(selected_date)}", status_code=303)


@app.get("/admin")
def admin_panel(request: Request, staff_error: str = "", staff_message: str = ""):
    user, redirect = require_role(request, "admin")
    if redirect:
        return redirect
    lang = get_lang(request)
    service_rows = [clean_service(row, lang) for row in services.find({}).sort("name", 1)]
    slot_rows = [clean_slot(row) for row in time_slots.find({}).sort([("date", 1), ("start_time", 1)]).limit(80)]
    appointment_rows = [
        clean_appointment(row, lang)
        for row in appointments.find({}).sort("created_at", -1).limit(80)
    ]
    staff_rows = [
        clean_account(row)
        for row in accounts.find({"role": {"$in": ["admin", "staff"]}}).sort([("role", 1), ("full_name", 1)])
    ]
    return render(
        request,
        "admin.html",
        {
            "services": service_rows,
            "slots": slot_rows,
            "appointments": appointment_rows,
            "service_categories": SERVICE_CATEGORY_LABELS,
            "staff_accounts": staff_rows,
            "staff_error": staff_error,
            "staff_message": staff_message,
            "admin_user": user,
        },
    )


@app.post("/admin/staff")
def create_staff_account(
    request: Request,
    full_name: str = Form(...),
    username: str = Form(...),
    password: str = Form(...),
    role: str = Form("staff"),
):
    _user, redirect = require_role(request, "admin")
    if redirect:
        return redirect

    full_name = normalize(full_name)
    username = normalize(username)
    password = password.strip()
    if role not in {"admin", "staff"}:
        error = "Выберите корректную роль."
    elif not full_name or not username or not password:
        error = "Заполните ФИО, логин и пароль."
    elif password_rule_error(password):
        error = password_rule_error(password)
    elif accounts.find_one({"username": username}):
        error = "Этот логин уже занят."
    else:
        try:
            accounts.insert_one(
                {
                    "username": username,
                    "password_hash": hash_password(password),
                    "role": role,
                    "full_name": full_name,
                    "created_at": now_utc(),
                }
            )
        except DuplicateKeyError:
            error = "Этот логин уже занят."
        else:
            role_label = ROLE_LABELS.get(role, "Сотрудник").lower()
            message = f"Аккаунт {role_label}а создан."
            return RedirectResponse(url=f"/admin?staff_message={quote(message)}", status_code=303)

    return RedirectResponse(url=f"/admin?staff_error={quote(error)}", status_code=303)


@app.post("/admin/services")
def create_service(
    request: Request,
    name: str = Form(...),
    description: str = Form(""),
    duration: int = Form(30),
    category: str = Form("documents"),
):
    _user, redirect = require_role(request, "admin")
    if redirect:
        return redirect
    if category not in SERVICE_CATEGORY_LABELS:
        category = "documents"
    services.insert_one(
        {
            "name": normalize(name),
            "description": normalize(description),
            "duration": int(duration),
            "category": category,
            "is_active": True,
        }
    )
    return RedirectResponse(url="/admin", status_code=303)


@app.post("/admin/services/{service_id}/toggle")
def toggle_service(request: Request, service_id: str):
    _user, redirect = require_role(request, "admin")
    if redirect:
        return redirect
    service = services.find_one({"_id": oid(service_id)})
    if not service:
        raise HTTPException(status_code=404, detail="Услуга не найдена")
    services.update_one({"_id": service["_id"]}, {"$set": {"is_active": not service.get("is_active", True)}})
    return RedirectResponse(url="/admin", status_code=303)


@app.post("/admin/slots")
def create_slot(
    request: Request,
    slot_date: str = Form(...),
    start_time: str = Form(...),
    end_time: str = Form(...),
):
    _user, redirect = require_role(request, "admin")
    if redirect:
        return redirect
    time_slots.insert_one(
        {
            "date": slot_date,
            "start_time": start_time,
            "end_time": end_time,
            "is_available": True,
        }
    )
    return RedirectResponse(url="/admin", status_code=303)


@app.post("/admin/slots/{slot_id}/toggle")
def toggle_slot(request: Request, slot_id: str):
    _user, redirect = require_role(request, "admin")
    if redirect:
        return redirect
    slot = time_slots.find_one({"_id": oid(slot_id)})
    if not slot:
        raise HTTPException(status_code=404, detail="Слот не найден")
    time_slots.update_one({"_id": slot["_id"]}, {"$set": {"is_available": not slot.get("is_available", True)}})
    return RedirectResponse(url="/admin", status_code=303)


@app.post("/admin/appointments/{appointment_id}/status")
def update_admin_status(
    request: Request,
    appointment_id: str,
    status: str = Form(...),
):
    _user, redirect = require_role(request, "admin")
    if redirect:
        return redirect
    appointment = appointments.find_one({"_id": oid(appointment_id)})
    if appointment is None:
        raise HTTPException(status_code=404, detail="Запись не найдена")
    if status not in STATUS_LABELS:
        raise HTTPException(status_code=400, detail="Некорректный статус")
    appointments.update_one({"_id": appointment["_id"]}, {"$set": {"status": status}})
    if appointment.get("time_slot_id"):
        time_slots.update_one(
            {"_id": appointment["time_slot_id"]},
            {"$set": {"is_available": status == "cancelled"}},
        )
    return RedirectResponse(url="/admin", status_code=303)
