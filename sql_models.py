from sqlalchemy import Column, Integer, String, Float, Boolean, ForeignKey, DateTime, UniqueConstraint
from sqlalchemy.orm import declarative_base, relationship
from datetime import datetime, timezone

Base = declarative_base()
####################################################
class Offer(Base):
    __tablename__ = 'offers'

    id = Column(Integer, primary_key=True)  # main
    original_id = Column(String(256),unique=False,nullable=False)  # id wg oferty
    title = Column(String(256),nullable=False)
    remote_interview = Column(Boolean)
    published_at = Column(DateTime,nullable=False)  # data publikacji oferty
    open_to_hire_ukrainians = Column(Boolean)
    category_id = Column(Integer, ForeignKey('categories.id'),nullable=False)
    experience_level_id = Column(Integer, ForeignKey('experience_levels.id'),nullable=False)
    workplace_type_id = Column(Integer, ForeignKey('workplace_types.id'),nullable=False)
    working_time_id = Column(Integer, ForeignKey('working_times.id'),nullable=False)
    company_id = Column(Integer, ForeignKey('companies.id'),nullable=False)
    offerent_id = Column(Integer, ForeignKey('offerents.id'),nullable=False)

    # relacje:
    slugs = relationship("Slug", back_populates="offers", cascade="all, delete-orphan")
    
    workplace_type = relationship("WorkplaceType", back_populates="offers")
    working_time = relationship("WorkingTime", back_populates="offers")
    experience_level = relationship("ExperienceLevel", back_populates="offers")
    employment_types = relationship("EmploymentType", back_populates="offers")
    
    category = relationship("Category", back_populates="offers")
 
    company = relationship("Company", back_populates="offers")
    offerent = relationship("Offerent", back_populates="offers")
 
    locations = relationship("Location", secondary="offer_location_association", back_populates="offers")

    required_skills = relationship("Skill", secondary="required_skill_association", back_populates="required_offers")
    nice_to_have_skills = relationship("Skill", secondary="nice_to_have_skill_association", back_populates="nice_to_have_offers")
    languages = relationship("Language", secondary="language_association", back_populates="offers")

    scraper_data = relationship("Scraper", back_populates="offers", uselist=False, cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("original_id", "published_at", name="_original_id_published_at_uc"),  # Unikalność kombinacji original_id i published_at
        {'sqlite_autoincrement': True},  # Dodanie autoinkrementacji w SQLite
    )

####################################################
class Slug(Base):
    __tablename__ = 'slugs'
    offer_id = Column(Integer, ForeignKey('offers.id'), primary_key=True)
    slug = Column(String(256), primary_key=True, nullable=False)
    
    offers = relationship("Offer", back_populates="slugs")
    # Kombinacja offer_id i location_id zapewnia unikalność powiązania
    __table_args__ = (UniqueConstraint('offer_id', 'slug', name='_offer_slug_unique'),)
####################################################
class WorkplaceType(Base):
    __tablename__ = 'workplace_types'
    id = Column(Integer, primary_key=True)
    name = Column(String(20),unique=True,nullable=False)

    offers = relationship("Offer", back_populates="workplace_type")

    __table_args__ = (
        {'sqlite_autoincrement': True},  # Dodanie autoinkrementacji w SQLite
    )
####################################################
class WorkingTime(Base):
    __tablename__ = 'working_times'
    id = Column(Integer, primary_key=True)
    name = Column(String(20),unique=True,nullable=False)

    offers = relationship("Offer", back_populates="working_time")

    __table_args__ = (
        {'sqlite_autoincrement': True},  # Dodanie autoinkrementacji w SQLite
    )
####################################################
class ExperienceLevel(Base):
    __tablename__ = 'experience_levels'
    id = Column(Integer, primary_key=True)
    name = Column(String(20),unique=True,nullable=False)

    offers = relationship("Offer", back_populates="experience_level")

    __table_args__ = (
        {'sqlite_autoincrement': True},  # Dodanie autoinkrementacji w SQLite
    )
####################################################
class Category(Base):
    __tablename__ = 'categories'
    id = Column(Integer, primary_key=True)
    name = Column(String(50),unique=True,nullable=False)   

    offers = relationship("Offer", back_populates="category")

    __table_args__ = (
        {'sqlite_autoincrement': True},  # Dodanie autoinkrementacji w SQLite
    )   
####################################################
class Company(Base):
    __tablename__ = 'companies'
    id = Column(Integer, primary_key=True)
    name = Column(String(50),unique=True,nullable=False)
    # url = Column(String(256))
    logo_url = Column(String(256))

    offers = relationship("Offer", back_populates="company")
    locations = relationship("Location", back_populates="company")

    __table_args__ = (
        {'sqlite_autoincrement': True},  # Dodanie autoinkrementacji w SQLite
    )
####################################################
class Location(Base):
    __tablename__ = 'locations'
    id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey('companies.id'))
    city = Column(String(50))
    street = Column(String(50))
    latitude = Column(Float)
    longitude = Column(Float)
    is_main = Column(Boolean, default=False)  # znacznik głównej lokalizacji

    
    company = relationship("Company", back_populates="locations")
    offers = relationship("Offer", secondary="offer_location_association", back_populates="locations")

    __table_args__ = (
        {'sqlite_autoincrement': True},  # Dodanie autoinkrementacji w SQLite
    )
####################################################
class OfferLocationAssociation(Base):
    __tablename__ = 'offer_location_association'
    
    offer_id = Column(Integer, ForeignKey('offers.id'),primary_key=True)
    location_id = Column(Integer, ForeignKey('locations.id'),primary_key=True)

####################################################
class Offerent(Base):
    __tablename__ = 'offerents'
    id = Column(Integer, primary_key=True)
    name = Column(String(50),unique=True,nullable=False)
    url = Column(String(256))

    offers = relationship("Offer", back_populates="offerent")

    __table_args__ = (
        {'sqlite_autoincrement': True},  # Dodanie autoinkrementacji w SQLite
    )
####################################################
class Skill(Base):
    __tablename__ = 'skills'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(50), unique=True,nullable=False)  # unikamy powtarzających się umiejętności

    # Relacja wiele-do-wielu z ofertami
    required_offers = relationship("Offer", secondary="required_skill_association", back_populates="required_skills")
    nice_to_have_offers = relationship("Offer", secondary="nice_to_have_skill_association", back_populates="nice_to_have_skills")

    __table_args__ = (
        {'sqlite_autoincrement': True},  # Dodanie autoinkrementacji w SQLite
    )   
####################################################
class RequiredSkillAssociation(Base):
    __tablename__ = 'required_skill_association'

    offer_id = Column(Integer, ForeignKey('offers.id'),primary_key=True)
    skill_id = Column(Integer, ForeignKey('skills.id'),primary_key=True)
    level = Column(Integer, nullable=True)  # poziom umiejętności (np. 1-5)
####################################################
class NiceToHaveSkillAssociation(Base):
    __tablename__ = 'nice_to_have_skill_association'

    offer_id = Column(Integer, ForeignKey('offers.id'),primary_key=True)
    skill_id = Column(Integer, ForeignKey('skills.id'),primary_key=True)
    level = Column(Integer, nullable=True)  # poziom umiejętności (np. 1-5)

####################################################
class EmploymentType(Base):
    __tablename__ = 'employment_types'
    id = Column(Integer, primary_key=True)

    offer_id = Column(Integer, ForeignKey('offers.id'))
    type = Column(String(10))
    currency = Column(String(10))
    unit = Column(String(10))
    gross = Column(Boolean)

    from_amount = Column(Integer)
    to_amount = Column(Integer)
    from_pln = Column(Float)
    to_pln = Column(Float)
    from_usd = Column(Float)
    to_usd = Column(Float)
    from_eur = Column(Float)
    to_eur = Column(Float)
    from_gbp = Column(Float)
    to_gbp = Column(Float)
    from_chf = Column(Float)
    to_chf = Column(Float)

    offers = relationship("Offer", back_populates="employment_types")

    __table_args__ = (
        {'sqlite_autoincrement': True},  # Dodanie autoinkrementacji w SQLite
    )
####################################################
class Language(Base):
    __tablename__ = 'languages'
    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    code = Column(String(10),unique=True,nullable=False)
    level = Column(String(10))

    offers = relationship("Offer", secondary="language_association", back_populates="languages")

    __table_args__ = (
        UniqueConstraint("code", "level", name="_code_level_unique"),
        {'sqlite_autoincrement': True},  # Dodanie autoinkrementacji w SQLite
    )
####################################################
class LanguageAssociation(Base):
    __tablename__ = 'language_association'
    offer_id = Column(Integer, ForeignKey('offers.id'),primary_key=True)
    language_id = Column(Integer, ForeignKey('languages.id'),primary_key=True)

####################################################
class Scraper(Base):
    __tablename__ = 'scraper'

    offer_id = Column(Integer, ForeignKey('offers.id'), primary_key=True)
    scraped_at = Column(DateTime, nullable=False)
    status = Column(String(50), nullable=False)
    url = Column(String(256),nullable=True)
    notes = Column(String,nullable=True)  # Dodatkowe notatki dotyczące oferty
    experience_description = Column(String,nullable=True)  # Opis doświadczenia
    years_of_experience = Column(String,nullable=True)  # Liczba lat doświadczenia
    interview_mode = Column(String,nullable=True)  # Tryb rozmowy kwalifikacyjnej (np. "online", "stacjonarnie")
    position_title = Column(String,nullable=True)  # Tytuł stanowiska (np. "Senior Developer")
    position_level = Column(String,nullable=True)  # Poziom stanowiska (np. "Senior", "Junior")
    responsibilities = Column(String,nullable=True)  # Obowiązki związane z ofertą
    requirements = Column(String,nullable=True)  # Wymagania dotyczące oferty
    benefits = Column(String,nullable=True)  # Korzyści oferowane przez pracodawcę
    industry = Column(String,nullable=True)  # Branża, w której działa firma
    company_size = Column(String,nullable=True)  # Rozmiar firmy (np. "mała", "średnia", "duża")

    offers = relationship("Offer", back_populates="scraper_data")    
####################################################
class ImportedFile(Base):
    __tablename__ = 'imported_files'

    id = Column(Integer, primary_key=True)
    filename = Column(String(256), unique=True, nullable=False)
    # imported_at = Column(DateTime, default=datetime.utcnow)
    imported_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    status = Column(String(50), nullable=True)  # np. "all_ok", "partial", "failed"

    # Jeśli chcesz zapisywać statystyki:
    lines_total = Column(Integer, default=0)
    lines_ok = Column(Integer, default=0)
    lines_duplikate = Column(Integer, default=0)
    lines_failed = Column(Integer, default=0)