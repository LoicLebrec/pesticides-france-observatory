import requests
import time
import re


class PubChemConnector:
    BASE_URL = "https://pubchem.ncbi.nlm.nih.gov/rest/pug"
    VIEW_URL = "https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/data/compound"

    def __init__(self):
        self.last_call = 0
        self.delay = 0.35

    def _wait(self):
        elapsed = time.time() - self.last_call
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)
        self.last_call = time.time()

    def get_details_from_cas(self, cas):
        self._wait()
        url_cid = f"{self.BASE_URL}/compound/name/{cas}/cids/JSON"
        try:
            r = requests.get(url_cid, timeout=10)
            if r.status_code != 200: return None
            cid = r.json()['IdentifierList']['CID'][0]

            self._wait()
            url_props = f"{self.BASE_URL}/compound/cid/{cid}/property/MolecularFormula,MolecularWeight/JSON"
            r_props = requests.get(url_props, timeout=10)
            props = r_props.json()['PropertyTable']['Properties'][0]
            return {'cid': cid, 'formula': props.get('MolecularFormula'), 'weight': props.get('MolecularWeight')}
        except:
            return None

    def get_ghs_classification(self, cid):
        if not cid: return []
        self._wait()
        url = f"{self.VIEW_URL}/{cid}/JSON"
        ghs_codes = set()

        try:
            r = requests.get(url, timeout=15)
            if r.status_code == 200:
                # CONVERTIR TOUT LE JSON EN TEXTE SIMPLE
                full_text = r.text

                # Regex : Cherche H suivi de 3 chiffres (ex: H300, H350, H410)
                # On évite H2O ou H1N1 en forçant 3 chiffres
                matches = re.findall(r'"String":\s*"(H\d{3}[a-zA-Z]?)', full_text)

                for code in matches:
                    # On garde surtout les codes de danger (H3xx = Santé, H4xx = Environnement)
                    if code.startswith("H3") or code.startswith("H4"):
                        ghs_codes.add(code)

        except Exception:
            pass

        return list(ghs_codes)