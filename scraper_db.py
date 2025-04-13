from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from sql_models import Slug, Scraper, Skill, RequiredSkillAssociation, NiceToHaveSkillAssociation
from datetime import datetime, timezone

class Database:
    def __init__(self, db_url):
        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)

    ##########################################
    def get_unscraped_slugs(self):
        session = self.Session()
        try:
            scraped_ids = {row.offer_id for row in session.query(Scraper.offer_id).all()}
            offers = (
                session.query(Slug)
                .filter(~Slug.offer_id.in_(scraped_ids))
                .order_by(Slug.offer_id, Slug.slug)
                .all()
            )

            seen = set()
            unique_slugs = []
            for slug in offers:
                if slug.offer_id not in seen:
                    seen.add(slug.offer_id)
                    unique_slugs.append(slug)
            return unique_slugs[:5]
        finally:
            session.close()

    ##########################################
    def save_scraper_entry(self, offer_id, status, url, notes,
                           experience_description=None,
                           years_of_experience=None,
                           interview_mode=None,
                           position_title=None,
                           position_level=None,
                           responsibilities=None,
                           requirements=None,
                           benefits=None,
                           industry=None,
                           company_size=None):
        session = self.Session()
        try:
            scraper_entry = Scraper(
                offer_id=offer_id,
                scraped_at=datetime.now(timezone.utc),
                status=status,
                url=url,
                notes=notes,
                experience_description=experience_description,
                years_of_experience=years_of_experience,
                interview_mode=interview_mode,
                position_title=position_title,
                position_level=position_level,
                responsibilities=responsibilities,
                requirements=requirements,
                benefits=benefits,
                industry=industry,
                company_size=company_size
            )
            session.add(scraper_entry)
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    ##########################################
    def get_required_skills_for_offer(self, offer_id):
        session = self.Session()
        try:
            result = (
                session.query(Skill)
                .join(RequiredSkillAssociation)
                .filter(RequiredSkillAssociation.offer_id == offer_id)
                .all()
            )
            return result
        finally:
            session.close()

    ##########################################
    def update_skill_level(self, offer_id, skill_id, level):
        session = self.Session()
        try:
            assoc = session.query(RequiredSkillAssociation).filter_by(offer_id=offer_id, skill_id=skill_id).first()
            if assoc:
                assoc.level = level
            else:
                assoc = RequiredSkillAssociation(offer_id=offer_id, skill_id=skill_id, level=level)
                session.add(assoc)
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    ##########################################
    def add_or_update_nice_to_have_skill(self, offer_id, skill_id, level=1):
        session = self.Session()
        try:
            assoc = session.query(NiceToHaveSkillAssociation).filter_by(offer_id=offer_id, skill_id=skill_id).first()
            if assoc:
                assoc.level = level
            else:
                assoc = NiceToHaveSkillAssociation(offer_id=offer_id, skill_id=skill_id, level=level)
                session.add(assoc)
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
