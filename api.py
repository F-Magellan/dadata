from dadata import DadataAsync
from typing import Optional, Union

#==============================================================================#

class Dadata(DadataAsync):

    def __init__(self, token: str, secret: str):
        super().__init__(token, secret)

    # ----------------------------------------------------------------- #

    async def suggest(self, name: str, query: str, single: bool = False, **kwargs) -> Union[list[dict], Optional[dict]]:
        results = await super().suggest(name, query, **kwargs)
        if single:
            return results[0] if results else None
        return results

    async def find(self, name: str, query: str, single: bool = False, **kwargs) -> Union[list[dict], Optional[dict]]:
        results = await super().find_by_id(name, query, **kwargs)
        if single:
            return results[0] if results else None
        return results

    async def geolocate(self, name: str, lat: float, lon: float, single: bool = False, **kwargs) -> Union[list[dict], Optional[dict]]:
        results = await super().geolocate(name, lat, lon, **kwargs)
        if single:
            return results[0] if results else None
        return results

    # ----------------------------------------------------------------- #

    async def address(self, address: str, single: bool = False, **kwargs) -> Union[list[dict], Optional[dict]]:
        return await self.suggest('address', address, single, **kwargs)

    async def company(self, compname: str, get_full=True, single: bool = False, **kwargs) -> Union[list[dict], Optional[dict]]:
        res = await self.suggest('party', compname, single, **kwargs)
        if not get_full or not res:
            return res
        if single:
            inn = res['data'].get('inn', res['data'].get('ogrn', None))
            if inn:
                return await self.company_by_inn(inn, True)
            return res
        else:
            out = []
            for comp in res:
                inn = comp['data'].get('inn', res['data'].get('ogrn', None))
                if inn:
                    more_data = await self.company_by_inn(inn, True)
                    out.append(more_data)
                else:
                    out.append(comp)
            return out

    async def company_by_inn(self, query: str, single: bool = False, **kwargs) -> Union[list[dict], Optional[dict]]:
        return await self.find('party', query, single, **kwargs)    

    async def bank(self, query: str, get_full=True, single: bool = False, **kwargs) -> Union[list[dict], Optional[dict]]:
        res = await self.suggest('bank', query, single, **kwargs)
        if not get_full or not res:
            return res
        if single:
            inn = res['data'].get('bic', None) or res['data'].get('swift', None) or res['data'].get('inn', None)
            if inn:
                return await self.bank_by_inn(inn, True)
            return res
        else:
            out = []
            for comp in res:
                inn = res['data'].get('bic', None) or res['data'].get('swift', None) or res['data'].get('inn', None)
                if inn:
                    more_data = await self.bank_by_inn(inn, True)
                    out.append(more_data)
                else:
                    out.append(comp)

    async def bank_by_inn(self, query: str, single: bool = False, **kwargs) -> Union[list[dict], Optional[dict]]:
        return await self.find('bank', query, single, **kwargs)
