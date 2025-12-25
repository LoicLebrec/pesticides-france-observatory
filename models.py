from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Substance(Base):
    __tablename__ = 'substance'

    id = Column(Integer, primary_key=True)
    cas_number = Column(String, unique=True, index=True, nullable=False)
    nom_ephy = Column(String)
    fonction = Column(String)
    cid_pubchem = Column(Integer, nullable=True)
    masse_molaire = Column(Float, nullable=True)
    formule = Column(String, nullable=True)

    toxicites = relationship("Toxicite", back_populates="substance", cascade="all, delete-orphan")


class Toxicite(Base):
    __tablename__ = 'toxicite'

    id = Column(Integer, primary_key=True)
    substance_id = Column(Integer, ForeignKey('substance.id'))
    source_db = Column(String)
    categorie = Column(String)
    parametre = Column(String)
    valeur = Column(String)
    unite = Column(String, nullable=True)

    substance = relationship("Substance", back_populates="toxicites")


def init_db(db_path='sqlite:///phyto_data.db'):
    engine = create_engine(db_path)
    Base.metadata.create_all(engine)
    return engine