import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Union, TextIO
from sqlalchemy.orm import Session

from sql_models import (
    Offer, Category, ExperienceLevel, WorkplaceType, WorkingTime,
    Company, Location, Offerent, Skill, Language,
    Slug, EmploymentType, RequiredSkillAssociation, NiceToHaveSkillAssociation,
    LanguageAssociation, OfferLocationAssociation, ImportedFile
)

# logging.basicConfig(level=logging.INFO)

###########################################
def detect_version(data: dict) -> str:
    if "guid" in data and "slug" in data and "publishedAt" in data:
        return "v1"
    elif "guid" not in data and "slug" in data and "publishedAt" in data:
        return "v2"
    elif "id" in data and "published_at" in data:
        return "v3"
    return "unknown"

###########################################
def get_or_create(session, model, defaults=None, **kwargs):
    instance = session.query(model).filter_by(**kwargs).first()
    if instance:
        return instance
    params = dict((k, v) for k, v in kwargs.items())
    if defaults:
        params.update(defaults)
    instance = model(**params)
    session.add(instance)
    session.flush()
    return instance

###########################################
def import_offer_v1(data: dict, session: Session, line_number: int):
    # od 2025-03-21
    published_at = (
        datetime.fromisoformat(data["publishedAt"].replace("Z", "+00:00"))
        if data.get("publishedAt") else None
    )

    if session.query(Offer).filter_by(original_id=data["guid"], published_at=published_at).first():
        logging.info(f"[{line_number}] Pomijam istniejącą ofertę: {data['guid']} ({published_at})")
        return True, 1 #ilosc duplikatów

    category = get_or_create(session, Category, id=data["categoryId"], name=f"Kategoria {data['categoryId']}")
    experience = get_or_create(session, ExperienceLevel, name=data["experienceLevel"])
    workplace = get_or_create(session, WorkplaceType, name=data["workplaceType"])
    working_time = get_or_create(session, WorkingTime, name=data["workingTime"])
    company = get_or_create(session, Company, name=data["companyName"], defaults={"logo_url": data.get("companyLogoThumbUrl")})
    offerent = get_or_create(session, Offerent, name="JustJoinIt", defaults={"url": "https://justjoin.it/"})

    offer = Offer(
        original_id=data["guid"],
        title=data["title"],
        remote_interview=data.get("remoteInterview"),
        published_at=datetime.fromisoformat(data["publishedAt"].replace("Z", "+00:00")) if data.get("publishedAt") else None,
        open_to_hire_ukrainians=data.get("openToHireUkrainians"),
        category_id=category.id,
        experience_level_id=experience.id,
        workplace_type_id=workplace.id,
        working_time_id=working_time.id,
        company_id=company.id,
        offerent_id=offerent.id
    )
    session.add(offer)
    session.flush()

    multilocations = data.get("multilocation")
    locations = []

    if multilocations:
        for i, loc in enumerate(multilocations):
            session.add(Slug(offer_id=offer.id, slug=loc["slug"]))
            location = get_or_create(
                session, Location,
                company_id=company.id,
                city=loc["city"], street=loc["street"],
                latitude=loc["latitude"], longitude=loc["longitude"],
                is_main=(i == 0)
            )
            locations.append(location)
    else:
        slug_value = data.get("slug")
        if isinstance(slug_value, str):
            session.add(Slug(offer_id=offer.id, slug=slug_value))

        location = get_or_create(
            session, Location,
            company_id=company.id,
            city=data["city"], street=data["street"],
            latitude=data["latitude"], longitude=data["longitude"],
            is_main=True
        )
        locations = [location]

    seen_locations = set()
    for loc in locations:
        key = (offer.id, loc.id)
        if key not in seen_locations:
            session.add(OfferLocationAssociation(offer_id=offer.id, location_id=loc.id))
            seen_locations.add(key)

    for skill in data.get("requiredSkills", []):
        s = get_or_create(session, Skill, name=skill)
        session.add(RequiredSkillAssociation(offer_id=offer.id, skill_id=s.id,level=0))

    nice_skills = data.get("niceToHaveSkills") or []
    for skill in nice_skills:
        s = get_or_create(session, Skill, name=skill)
        session.add(NiceToHaveSkillAssociation(offer_id=offer.id, skill_id=s.id))

    for lang in data.get("languages", []):
        l = get_or_create(session, Language, code=lang["code"], defaults={"level": lang["level"]})
        session.add(LanguageAssociation(offer_id=offer.id, language_id=l.id))

    for et in data.get("employmentTypes", []):
        session.add(EmploymentType(
            offer_id=offer.id,
            type=et["type"],
            currency=et["currency"],
            unit=et["unit"],
            gross=et["gross"],
            from_amount=et["from"], to_amount=et["to"],
            from_pln=et.get("fromPln"), to_pln=et.get("toPln"),
            from_usd=et.get("fromUsd"), to_usd=et.get("toUsd"),
            from_eur=et.get("fromEur"), to_eur=et.get("toEur"),
            from_gbp=et.get("fromGbp"), to_gbp=et.get("toGbp"),
            from_chf=et.get("fromChf"), to_chf=et.get("toChf")
        ))

    logging.info(f"[{line_number}] ✅ Dodano ofertę (v1): {data['guid']} - {data['title']}")
    return True, 0 #ilosc duplikatów

###########################################
def import_offer_v2(data: dict, session: Session, line_number: int):
    # od 2023-01-01
    # do 2025-03-21
    published_at = (
        datetime.fromisoformat(data["publishedAt"].replace("Z", "+00:00"))
        if data.get("publishedAt") else None
    )

    if session.query(Offer).filter_by(original_id=data["slug"], published_at=published_at).first():
        logging.info(f"[{line_number}] Pomijam istniejącą ofertę: {data['slug']} ({published_at})")
        return True, 1 #ilosc duplikatów

    category = get_or_create(session, Category, id=data["categoryId"], name=f"Kategoria {data['categoryId']}")
    experience = get_or_create(session, ExperienceLevel, name=data["experienceLevel"])
    workplace = get_or_create(session, WorkplaceType, name=data["workplaceType"])
    working_time = get_or_create(session, WorkingTime, name=data["workingTime"])
    company = get_or_create(session, Company, name=data["companyName"], defaults={"logo_url": data.get("companyLogoThumbUrl")})
    offerent = get_or_create(session, Offerent, name="JustJoinIt", defaults={"url": "https://justjoin.it/"})

    offer = Offer(
        original_id=data["slug"],
        title=data["title"],
        remote_interview=data.get("remoteInterview"),
        published_at=datetime.fromisoformat(data["publishedAt"].replace("Z", "+00:00")) if data.get("publishedAt") else None,
        open_to_hire_ukrainians=data.get("openToHireUkrainians"),
        category_id=category.id,
        experience_level_id=experience.id,
        workplace_type_id=workplace.id,
        working_time_id=working_time.id,
        company_id=company.id,
        offerent_id=offerent.id
    )
    session.add(offer)
    session.flush()

    multilocations = data.get("multilocation")
    locations = []

    if multilocations:
        for i, loc in enumerate(multilocations):
            session.add(Slug(offer_id=offer.id, slug=loc["slug"]))
            location = get_or_create(
                session, Location,
                company_id=company.id,
                city=loc["city"], street=loc["street"],
                latitude=loc["latitude"], longitude=loc["longitude"],
                is_main=(i == 0)
            )
            locations.append(location)
    else:
        slug_value = data.get("slug")
        if isinstance(slug_value, str):
            session.add(Slug(offer_id=offer.id, slug=slug_value))

        location = get_or_create(
            session, Location,
            company_id=company.id,
            city=data["city"], street=data["street"],
            latitude=data["latitude"], longitude=data["longitude"],
            is_main=True
        )
        locations = [location]

    seen_locations = set()
    for loc in locations:
        key = (offer.id, loc.id)
        if key not in seen_locations:
            session.add(OfferLocationAssociation(offer_id=offer.id, location_id=loc.id))
            seen_locations.add(key)

    for skill in data.get("requiredSkills", []):
        s = get_or_create(session, Skill, name=skill)
        session.add(RequiredSkillAssociation(offer_id=offer.id, skill_id=s.id,level=0))

    nice_skills = data.get("niceToHaveSkills") or []
    for skill in nice_skills:
        s = get_or_create(session, Skill, name=skill)
        session.add(NiceToHaveSkillAssociation(offer_id=offer.id, skill_id=s.id))

    for lang in data.get("languages", []):
        l = get_or_create(session, Language, code=lang["code"], defaults={"level": lang["level"]})
        session.add(LanguageAssociation(offer_id=offer.id, language_id=l.id))

    for et in data.get("employmentTypes", []):
        session.add(EmploymentType(
            offer_id=offer.id,
            type=et["type"],
            currency=et["currency"],
            unit=et["unit"],
            gross=et["gross"],
            from_amount=et["from"], to_amount=et["to"],
            from_pln=et.get("fromPln"), to_pln=et.get("toPln"),
            from_usd=et.get("fromUsd"), to_usd=et.get("toUsd"),
            from_eur=et.get("fromEur"), to_eur=et.get("toEur"),
            from_gbp=et.get("fromGbp"), to_gbp=et.get("toGbp"),
            from_chf=et.get("fromChf"), to_chf=et.get("toChf")
        ))

    logging.info(f"[{line_number}] ✅ Dodano ofertę (v2): {data['slug']} - {data['title']}")
    return True, 0 #ilosc duplikatów

###########################################
def import_offer_v3(data: dict, session: Session, line_number: int):
    # do 2023-12-31
    
    published_at = (
        datetime.fromisoformat(data["published_at"].replace("Z", "+00:00"))
        if data.get("published_at") else None
    )

    if session.query(Offer).filter_by(original_id=data["id"], published_at=published_at).first():
        logging.info(f"[{line_number}] Pomijam istniejącą ofertę: {data['id']} ({published_at})")
        return True, 1 #ilosc duplikatów

    category = get_or_create(session, Category, id=0, name=f"Kategoria 0")
    experience = get_or_create(session, ExperienceLevel, name=data["experience_level"])
    workplace = get_or_create(session, WorkplaceType, name=data["workplace_type"])
    working_time = get_or_create(session, WorkingTime, name="unknown")
    company = get_or_create(session, Company, name=data["company_name"], defaults={"logo_url": data.get("company_logo_url")})
    offerent = get_or_create(session, Offerent, name="JustJoinIt", defaults={"url": "https://justjoin.it/"})

    offer = Offer(
        original_id=data["id"],
        title=data["title"],
        remote_interview=data.get("remote_interview"),
        published_at=datetime.fromisoformat(data["published_at"].replace("Z", "+00:00")) if data.get("published_at") else None,
        open_to_hire_ukrainians=data.get("open_to_hire_ukrainians", False),
        category_id=category.id,
        experience_level_id=experience.id,
        workplace_type_id=workplace.id,
        working_time_id=working_time.id,
        company_id=company.id,
        offerent_id=offerent.id
    )
    session.add(offer)
    session.flush()

    multilocations = data.get("multilocation")
    locations = []

    if multilocations:
        for i, loc in enumerate(multilocations):
            # Dodaj slug
            if "slug" in loc:
                session.add(Slug(offer_id=offer.id, slug=loc["slug"]))

            # Dodaj lokalizację
            location = get_or_create(session, Location,
                company_id=company.id,
                city=loc.get("city", "unknown"),
                street=loc.get("street", ""),
                latitude=float(loc["latitude"]) if loc.get("latitude") else None,
                longitude=float(loc["longitude"]) if loc.get("longitude") else None,
                is_main=(i == 0)
            )
            locations.append(location)
    else:
        # Dodaj slug (jeśli nie multilocation, slug może być w `id`)
        slug_value = data.get("id")
        if slug_value:
            session.add(Slug(offer_id=offer.id, slug=slug_value))

        # Dodaj pojedynczą lokalizację
        location = get_or_create(session, Location,
            company_id=company.id,
            city=data.get("city", "unknown"),
            street=data.get("street", ""),
            latitude=float(data["latitude"]),
            longitude=float(data["longitude"]),
            is_main=True
        )
        locations = [location]

    # Dodaj powiązania lokalizacji z ofertą
    seen_locations = set()
    for loc in locations:
        key = (offer.id, loc.id)
        if key not in seen_locations:
            session.add(OfferLocationAssociation(offer_id=offer.id, location_id=loc.id))
            seen_locations.add(key)

    for skill_obj in data.get("skills", []):
        s = get_or_create(session, Skill, name=skill_obj["name"])
        session.add(RequiredSkillAssociation(offer_id=offer.id, skill_id=s.id,level=0))

    for et in data.get("employment_types", []):
        salary = et.get("salary") or {}
        session.add(EmploymentType(
            offer_id=offer.id,
            type=et["type"],
            currency=salary.get("currency"),
            unit=et.get("unit", "month"),
            gross=salary.get("gross") if isinstance(salary, dict) else None,
            from_amount=salary.get("from"),
            to_amount=salary.get("to")
        ))

    logging.info(f"[{line_number}] ✅ Dodano ofertę (v3): {data['id']} - {data['title']}")
    return True, 0 #ilosc duplikatów


###########################################
def import_offers_from_jsonl(source: Union[str, Path, TextIO], session: Session, filename: str = None):
    if not filename:
        raise ValueError("Brakuje nazwy pliku - filename jest wymagany dla rejestracji importu.")
    if isinstance(source, (str, Path)):
        f = open(source, "r", encoding="utf-8")
        should_close = True
    else:
        f = (line.decode("utf-8") for line in source.readlines())
        should_close = False

    lines_total = 0
    lines_ok = 0
    lines_failed = 0
    lines_duplikate = 0

    try:
        for line_number, line in enumerate(f, start=1):
            lines_total += 1
            try:
                data = json.loads(line)
                version = detect_version(data)

                if version == "v1":
                    success, duplikate = import_offer_v1(data, session, line_number)
                    if success:
                        lines_ok += 1
                        lines_duplikate += duplikate
                    else:
                        lines_failed += 1
                elif version == "v2":
                    success, duplikate = import_offer_v2(data, session, line_number)
                    if success:
                        lines_ok += 1
                        lines_duplikate += duplikate
                    else:
                        lines_failed += 1
                elif version == "v3":
                    success, duplikate = import_offer_v3(data, session, line_number)
                    if success:
                        lines_ok += 1
                        lines_duplikate += duplikate
                    else:
                        lines_failed += 1
                else:
                    logging.warning(f"[{line_number}] ⚠️ Nieobsługiwana wersja: {version}")
                    lines_failed += 1

            except Exception as e:
                session.rollback()
                lines_failed += 1
                logging.exception(f"[{line_number}] ❌ Błąd importu: {e}")
                with open("offers_failed.jsonl", "a", encoding="utf-8") as fail_file:
                    fail_file.write(json.dumps(data, ensure_ascii=False) + "\n")

        session.commit()
        logging.info("✅ Import zakończony.")

        # Zakończenie importu — rejestracja w bazie
        if filename and lines_ok > 0:
            status = "all_ok" if lines_failed == 0 else "partial" if lines_ok > 0 else "failed"
            session.add(ImportedFile(
                filename=filename,
                imported_at=datetime.now(timezone.utc),
                status=status,
                lines_total=lines_total,
                lines_ok=lines_ok,
                lines_duplikate=lines_duplikate,
                lines_failed=lines_failed
            ))
            session.commit()
            logging.info(f"📦 Zarejestrowano plik: {filename} | status: {status} | total: {lines_total} | ok: {lines_ok} | duplikate: {lines_duplikate} | błędne: {lines_failed}")

    finally:
        if should_close:
            f.close()

    return lines_ok, lines_failed, lines_duplikate, lines_total