import pandas as pd
import logging
import re

logger = logging.getLogger("EFSA")


class EfsaConnector:
    def __init__(self, char_file, ref_file):
        self.char_file = char_file
        self.ref_file = ref_file
        self.df_subst = None
        self.df_ref = None

    def _universal_decode(self, val):
        if pd.isna(val): return ""
        val_str = str(val).strip()
        if "_x" in val_str:
            try:
                val_str = re.sub(r'_x([0-9a-fA-F]{4})_', lambda m: chr(int(m.group(1), 16)), val_str)
            except:
                pass
        if val_str.endswith('.0'):
            val_str = val_str[:-2]
        return val_str.strip()

    def _normalize_cas(self, cas):
        cas = self._universal_decode(cas)
        return cas.lstrip('0')

    def load_data(self):
        logger.info("Chargement et Décodage Intégral EFSA...")
        try:
            self.df_subst = pd.read_excel(self.char_file, engine='openpyxl')
            self.df_ref = pd.read_excel(self.ref_file, engine='openpyxl')

            # Normalisation des noms de colonnes
            self.df_subst.columns = [str(c).lower().strip() for c in self.df_subst.columns]
            self.df_ref.columns = [str(c).lower().strip() for c in self.df_ref.columns]

            # Mapping colonnes Substance
            col_cas = next((c for c in ['casnumber', 'cas_number', 'cas'] if c in self.df_subst.columns), None)
            col_name = next((c for c in ['substance', 'name'] if c in self.df_subst.columns), None)

            if col_cas:
                self.df_subst['cas_key'] = self.df_subst[col_cas].apply(self._normalize_cas)
            if col_name:
                self.df_subst['substance_key'] = self.df_subst[col_name].apply(self._universal_decode)

            # Nettoyage Ref (pour jointure)
            if 'substance' in self.df_ref.columns:
                self.df_ref['substance_key'] = self.df_ref['substance'].apply(self._universal_decode)

        except Exception as e:
            logger.critical(f"Erreur chargement EFSA: {e}")

    def get_tox_values(self, cas_input):
        if self.df_subst is None or 'cas_key' not in self.df_subst.columns: return []

        cas_clean = self._normalize_cas(cas_input)
        match = self.df_subst[self.df_subst['cas_key'] == cas_clean]

        if match.empty: return []

        found_values = pd.DataFrame()

        # Jointure par Nom (Le plus fiable ici)
        if 'substance_key' in match.columns and 'substance_key' in self.df_ref.columns:
            names = match['substance_key'].unique()
            found_values = self.df_ref[self.df_ref['substance_key'].isin(names)]

        results = []
        for _, row in found_values.iterrows():
            # --- CORRECTION ICI : Utilisation des colonnes vues dans votre log ---
            # On cherche 'assessment' au lieu de 'referencevaluetype'
            rtype = self._universal_decode(row.get('assessment') or row.get('referencevaluetype')).upper()
            rval = self._universal_decode(row.get('value') or row.get('referencevalue'))
            runit = self._universal_decode(row.get('unit') or row.get('referencevalueunit'))

            # On filtre les mots clés importants (ADI, ARfD, AOEL)
            if any(x in rtype for x in ['ADI', 'ARFD', 'AOEL', 'AAOEL']):
                results.append({'parametre': rtype, 'valeur': rval, 'unite': runit})

        return results