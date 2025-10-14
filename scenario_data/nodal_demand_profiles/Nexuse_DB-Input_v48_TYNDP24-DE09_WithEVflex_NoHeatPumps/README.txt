These nodal-hourly data include:

For Switzerland
- CH: Conventional demand for CH from EP2050+, split using population (2018-2050)
- CH: Electrolysis demand for CH from EP2050+, split using population (2018-2050, 2018 = 0)
- CH: eMobility demand from EP2050+ scaled from Maria's data (2018-2050)
	- Flex Pmax: 			set as equal to eMobility demand (2018-2020), scaled from Maria's data (2030-2050)
	- Flex Pmin: 			all zeros (2018-2020), scaled from Maria's data (2030-2050)
	- Flex Energy,daily: 	all zeros (2018-2020), scaled from Maria's data (2030-2050)
- CH: HeatPump demands are all zeros (2018-2050)
	- Flex Pmax: 			all zeros (2018-2050)
	- Flex Ecumul,max: 		all zeros (2018-2050)
	- Flex Ecumul,min: 		all zeros (2018-2050)

For Neighbors
- nonCH: Conventional demand from TYNDP2024 (2030-2050), or from historical data (2018-2020)
- nonCH: Electrolysis demand from TYNDP2024 (2030-2050), historical are all zeros
- nonCH: eMobility demand from TYNDP2024 (2030-2050, 2030 is zero), historical are all zeros
	- Flex Pmax: 			all zeros (2018-2020), set as equal to eMobility demand (2030-2050, 2030 is zero)
	- Flex Pmin: 			all zeros (2018-2050)
	- Flex Energy,daily: 	all zeros (2018-2050)
- nonCH: HeatPump demands are all zeros (2018-2050)
	- Flex Pmax: 			all zeros (2018-2050)
	- Flex Ecumul,max: 		all zeros (2018-2050)
	- Flex Ecumul,min: 		all zeros (2018-2050)