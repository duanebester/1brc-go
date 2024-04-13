package main

import (
	"fmt"
	"slices"

	"github.com/edsrzf/mmap-go"
)

type Measurement struct {
	Min   int
	Max   int
	Mean  int
	Sum   int64
	Count int64
}

type MemChunk struct {
	start int
	end   int
}

func splitMemory(mem mmap.MMap, n int) []MemChunk {
	var total = len(mem)
	chunkSize := total / n
	chunks := make([]MemChunk, n)

	chunks[0].start = 0
	for i := 1; i < n; i++ {
		for j := i * chunkSize; j < i*chunkSize+50; j++ {
			if mem[j] == '\n' {
				chunks[i-1].end = j
				chunks[i].start = j + 1
				break
			}
		}
	}
	chunks[n-1].end = total - 1
	return chunks
}

func printResultMap(totals map[string]*Measurement) {
	// Sort by city name
	keys := make([]string, 0, len(totals))
	for k := range totals {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	// Print result
	fmt.Print("{")
	total := len(keys)
	for idx, k := range keys {
		measurement := totals[k]
		mean := float64(measurement.Sum/10) / float64(measurement.Count)
		min := float64(measurement.Min) / float64(10)
		max := float64(measurement.Max) / float64(10)

		fmt.Printf("%s=%.1f/%.1f/%.1f", k, min, mean, max)
		if idx < total-1 {
			fmt.Print(", ")
		}
	}
	fmt.Println("}")
}

const BASELINE = "{Abha=-34.7/18.0/64.3, Abidjan=-20.7/26.0/75.8, Abéché=-19.6/29.4/85.6, Accra=-22.3/26.4/74.5, Addis Ababa=-35.4/16.0/64.0, Adelaide=-32.8/17.3/68.4, Aden=-20.8/29.1/76.5, Ahvaz=-23.0/25.4/79.2, Albuquerque=-36.7/14.0/66.5, Alexandra=-37.2/11.0/57.5, Alexandria=-28.3/20.0/72.0, Algiers=-33.8/18.2/67.9, Alice Springs=-33.6/21.0/72.6, Almaty=-43.5/10.0/58.2, Amsterdam=-39.0/10.2/58.8, Anadyr=-53.6/-6.9/40.6, Anchorage=-47.3/2.8/51.9, Andorra la Vella=-38.5/9.8/63.2, Ankara=-36.4/12.0/61.1, Antananarivo=-31.9/17.9/68.0, Antsiranana=-26.3/25.2/74.6, Arkhangelsk=-47.6/1.3/51.1, Ashgabat=-35.9/17.1/66.6, Asmara=-34.2/15.6/68.8, Assab=-19.3/30.5/80.1, Astana=-47.7/3.5/56.7, Athens=-29.9/19.2/70.9, Atlanta=-32.8/17.0/68.0, Auckland=-35.3/15.2/65.9, Austin=-32.2/20.7/68.8, Baghdad=-24.4/22.8/70.7, Baguio=-30.0/19.5/67.7, Baku=-41.2/15.1/64.9, Baltimore=-36.1/13.1/59.8, Bamako=-24.3/27.8/78.7, Bangkok=-18.9/28.6/79.3, Bangui=-21.9/26.0/74.8, Banjul=-22.1/26.0/74.4, Barcelona=-31.9/18.2/69.8, Bata=-24.5/25.1/76.0, Batumi=-34.1/14.0/64.0, Beijing=-41.9/12.9/60.5, Beirut=-29.5/20.9/72.1, Belgrade=-35.5/12.5/63.9, Belize City=-27.7/26.7/74.9, Benghazi=-31.8/19.9/68.0, Bergen=-45.5/7.7/57.9, Berlin=-38.6/10.3/60.6, Bilbao=-33.9/14.7/63.7, Birao=-24.3/26.5/75.9, Bishkek=-38.5/11.3/69.6, Bissau=-20.5/27.0/80.4, Blantyre=-34.0/22.2/81.3, Bloemfontein=-35.3/15.6/71.4, Boise=-36.2/11.4/59.4, Bordeaux=-34.8/14.2/59.8, Bosaso=-18.6/30.0/80.5, Boston=-38.9/10.9/59.2, Bouaké=-23.6/26.0/73.3, Bratislava=-38.4/10.5/60.5, Brazzaville=-22.7/25.0/73.0, Bridgetown=-23.5/27.0/77.9, Brisbane=-29.5/21.4/72.6, Brussels=-38.6/10.5/60.6, Bucharest=-39.6/10.8/65.2, Budapest=-40.4/11.3/58.3, Bujumbura=-25.7/23.8/78.9, Bulawayo=-31.2/18.9/69.2, Burnie=-36.6/13.1/63.6, Busan=-37.3/15.0/65.0, Cabo San Lucas=-28.1/23.9/73.3, Cairns=-25.7/25.0/74.3, Cairo=-23.9/21.4/69.1, Calgary=-43.6/4.4/54.7, Canberra=-35.3/13.1/63.3, Cape Town=-35.9/16.2/66.3, Changsha=-33.2/17.4/68.0, Charlotte=-36.3/16.1/73.6, Chiang Mai=-32.3/25.8/75.8, Chicago=-39.6/9.8/56.1, Chihuahua=-32.7/18.6/67.2, Chittagong=-22.5/25.9/77.3, Chișinău=-38.4/10.2/59.2, Chongqing=-32.8/18.6/65.3, Christchurch=-36.6/12.2/65.0, City of San Marino=-36.1/11.8/64.4, Colombo=-25.7/27.4/77.5, Columbus=-36.5/11.7/63.7, Conakry=-24.9/26.4/75.8, Copenhagen=-38.8/9.1/60.3, Cotonou=-26.3/27.2/79.0, Cracow=-43.2/9.3/61.3, Da Lat=-29.2/17.9/72.0, Da Nang=-33.3/25.8/80.3, Dakar=-28.3/24.0/76.6, Dallas=-27.3/19.0/72.2, Damascus=-34.6/17.0/67.3, Dampier=-23.1/26.4/82.5, Dar es Salaam=-25.1/25.8/72.1, Darwin=-21.3/27.6/76.4, Denpasar=-28.0/23.7/73.0, Denver=-41.8/10.4/58.8, Detroit=-38.4/10.0/57.9, Dhaka=-24.0/25.9/72.9, Dikson=-59.2/-11.1/38.4, Dili=-28.1/26.6/80.9, Djibouti=-20.0/29.9/80.3, Dodoma=-27.6/22.7/72.4, Dolisie=-25.9/24.0/78.2, Douala=-23.8/26.7/75.9, Dubai=-21.8/26.9/82.5, Dublin=-46.6/9.8/62.0, Dunedin=-35.8/11.1/62.3, Durban=-31.5/20.6/70.0, Dushanbe=-36.1/14.7/64.5, Edinburgh=-41.2/9.3/59.9, Edmonton=-50.5/4.2/58.6, El Paso=-29.7/18.1/65.5, Entebbe=-29.9/21.0/67.9, Erbil=-29.2/19.5/70.6, Erzurum=-45.5/5.1/57.1, Fairbanks=-52.0/-2.3/49.1, Fianarantsoa=-32.2/17.9/73.1, Flores,  Petén=-27.9/26.4/75.9, Frankfurt=-42.1/10.6/60.8, Fresno=-30.2/17.9/69.3, Fukuoka=-36.7/17.0/67.2, Gaborone=-31.6/21.0/69.3, Gabès=-30.4/19.5/66.6, Gagnoa=-22.7/26.0/75.3, Gangtok=-30.1/15.2/70.9, Garissa=-22.5/29.3/82.2, Garoua=-20.0/28.3/78.5, George Town=-21.1/27.9/75.4, Ghanzi=-27.2/21.4/70.9, Gjoa Haven=-65.4/-14.4/36.0, Guadalajara=-27.2/20.9/69.2, Guangzhou=-31.8/22.4/79.0, Guatemala City=-27.6/20.4/69.2, Halifax=-42.0/7.5/59.1, Hamburg=-39.0/9.7/59.4, Hamilton=-39.1/13.8/59.8, Hanga Roa=-30.0/20.5/81.0, Hanoi=-24.3/23.6/74.8, Harare=-29.5/18.4/66.1, Harbin=-43.6/5.0/53.4, Hargeisa=-27.2/21.7/73.8, Hat Yai=-23.4/27.0/76.7, Havana=-23.9/25.2/79.7, Helsinki=-43.5/5.9/55.2, Heraklion=-33.0/18.9/68.8, Hiroshima=-32.0/16.3/68.1, Ho Chi Minh City=-22.1/27.4/77.2, Hobart=-37.4/12.7/65.7, Hong Kong=-27.2/23.3/73.9, Honiara=-25.7/26.5/74.5, Honolulu=-30.8/25.4/74.8, Houston=-27.1/20.8/70.9, Ifrane=-39.4/11.4/61.5, Indianapolis=-36.9/11.8/62.4, Iqaluit=-59.0/-9.3/43.8, Irkutsk=-48.2/1.0/53.4, Istanbul=-36.2/13.9/66.5, Jacksonville=-27.9/20.3/73.5, Jakarta=-20.8/26.7/76.2, Jayapura=-21.7/27.0/81.8, Jerusalem=-34.8/18.3/66.9, Johannesburg=-36.1/15.5/65.8, Jos=-26.5/22.8/72.9, Juba=-24.3/27.8/74.1, Kabul=-36.5/12.1/62.6, Kampala=-30.2/20.0/72.5, Kandi=-22.8/27.7/77.0, Kankan=-24.9/26.5/77.2, Kano=-25.2/26.4/74.8, Kansas City=-32.8/12.5/61.9, Karachi=-22.8/26.0/75.2, Karonga=-25.8/24.4/72.4, Kathmandu=-32.1/18.3/66.8, Khartoum=-22.9/29.9/80.0, Kingston=-22.7/27.4/77.1, Kinshasa=-24.8/25.3/78.1, Kolkata=-21.6/26.7/77.5, Kuala Lumpur=-19.1/27.3/82.2, Kumasi=-26.3/26.0/72.6, Kunming=-31.9/15.7/65.3, Kuopio=-45.8/3.4/54.4, Kuwait City=-27.5/25.7/76.8, Kyiv=-43.2/8.4/59.4, Kyoto=-35.3/15.8/64.0, La Ceiba=-22.5/26.2/77.0, La Paz=-26.8/23.7/72.7, Lagos=-26.8/26.8/78.4, Lahore=-26.2/24.3/71.5, Lake Havasu City=-27.5/23.7/72.8, Lake Tekapo=-39.8/8.7/56.7, Las Palmas de Gran Canaria=-29.2/21.2/68.4, Las Vegas=-30.1/20.3/67.5, Launceston=-36.0/13.1/61.7, Lhasa=-40.2/7.6/55.1, Libreville=-23.7/25.9/76.2, Lisbon=-29.2/17.5/76.4, Livingstone=-27.4/21.8/73.1, Ljubljana=-37.7/10.9/60.5, Lodwar=-19.1/29.3/83.3, Lomé=-21.5/26.9/74.1, London=-36.6/11.3/61.3, Los Angeles=-30.8/18.6/68.0, Louisville=-38.7/13.9/64.7, Luanda=-22.7/25.8/72.9, Lubumbashi=-30.5/20.8/70.8, Lusaka=-27.3/19.9/68.4, Luxembourg City=-39.0/9.3/61.5, Lviv=-42.3/7.8/60.7, Lyon=-38.1/12.5/63.6, Madrid=-37.6/15.0/65.2, Mahajanga=-23.1/26.3/78.2, Makassar=-27.1/26.7/81.3, Makurdi=-28.1/26.0/76.9, Malabo=-24.3/26.3/75.7, Malé=-18.6/28.0/74.2, Managua=-24.6/27.3/80.9, Manama=-20.6/26.5/74.4, Mandalay=-21.6/28.0/78.4, Mango=-21.6/28.1/76.9, Manila=-24.2/28.4/77.4, Maputo=-33.1/22.8/72.8, Marrakesh=-27.4/19.6/70.6, Marseille=-34.6/15.8/64.7, Maun=-27.6/22.4/70.5, Medan=-23.8/26.5/74.7, Mek'ele=-28.1/22.7/72.2, Melbourne=-36.6/15.1/67.7, Memphis=-33.9/17.2/64.1, Mexicali=-24.9/23.1/73.7, Mexico City=-32.9/17.5/65.2, Miami=-26.0/24.9/70.8, Milan=-37.7/13.0/62.1, Milwaukee=-41.8/8.9/56.2, Minneapolis=-43.2/7.8/58.5, Minsk=-44.7/6.7/56.1, Mogadishu=-25.1/27.1/76.4, Mombasa=-25.0/26.3/73.4, Monaco=-32.9/16.4/69.6, Moncton=-44.7/6.1/55.2, Monterrey=-30.9/22.3/71.1, Montreal=-43.2/6.8/56.1, Moscow=-42.4/5.8/54.3, Mumbai=-27.4/27.1/75.1, Murmansk=-46.9/0.6/49.2, Muscat=-23.6/28.0/78.6, Mzuzu=-29.5/17.7/72.0, N'Djamena=-20.1/28.3/76.7, Naha=-27.1/23.1/74.8, Nairobi=-34.0/17.8/66.4, Nakhon Ratchasima=-20.6/27.3/81.4, Napier=-36.0/14.6/62.4, Napoli=-37.7/15.9/66.0, Nashville=-34.5/15.4/66.4, Nassau=-28.6/24.6/76.0, Ndola=-30.8/20.3/70.1, New Delhi=-23.1/25.0/71.0, New Orleans=-30.5/20.7/73.7, New York City=-38.3/12.9/61.7, Ngaoundéré=-29.5/22.0/75.1, Niamey=-18.6/29.3/81.1, Nicosia=-32.1/19.7/71.8, Niigata=-35.1/13.9/67.5, Nouadhibou=-34.8/21.3/68.9, Nouakchott=-22.7/25.7/79.2, Novosibirsk=-48.7/1.7/51.4, Nuuk=-49.2/-1.4/50.5, Odesa=-40.1/10.7/59.2, Odienné=-26.5/26.0/77.9, Oklahoma City=-35.8/15.9/67.4, Omaha=-37.6/10.6/58.8, Oranjestad=-22.9/28.1/79.3, Oslo=-42.7/5.7/60.3, Ottawa=-46.1/6.6/54.8, Ouagadougou=-21.7/28.3/79.6, Ouahigouya=-22.1/28.6/76.3, Ouarzazate=-33.4/18.9/71.3, Oulu=-46.6/2.7/52.0, Palembang=-24.0/27.3/78.4, Palermo=-31.5/18.5/69.9, Palm Springs=-25.3/24.5/73.0, Palmerston North=-38.3/13.2/62.0, Panama City=-20.2/28.0/75.5, Parakou=-25.6/26.8/75.1, Paris=-38.1/12.3/69.5, Perth=-33.6/18.7/77.3, Petropavlovsk-Kamchatsky=-48.1/1.9/52.0, Philadelphia=-35.8/13.2/65.0, Phnom Penh=-20.9/28.3/77.2, Phoenix=-33.9/23.9/72.9, Pittsburgh=-40.6/10.8/64.1, Podgorica=-32.1/15.3/63.3, Pointe-Noire=-26.4/26.1/80.4, Pontianak=-24.0/27.7/78.8, Port Moresby=-21.1/26.9/79.1, Port Sudan=-21.0/28.4/81.3, Port Vila=-30.2/24.3/76.0, Port-Gentil=-24.2/26.0/76.2, Portland (OR)=-36.3/12.4/62.0, Porto=-30.9/15.7/63.8, Prague=-47.6/8.4/57.3, Praia=-26.4/24.4/72.7, Pretoria=-34.2/18.2/67.0, Pyongyang=-37.1/10.8/59.6, Rabat=-30.1/17.2/66.9, Rangpur=-24.5/24.4/75.7, Reggane=-24.0/28.3/82.4, Reykjavík=-47.0/4.3/54.6, Riga=-42.8/6.2/63.7, Riyadh=-26.9/26.0/77.2, Rome=-40.2/15.2/65.3, Roseau=-25.2/26.2/75.8, Rostov-on-Don=-42.1/9.9/59.7, Sacramento=-38.3/16.3/66.2, Saint Petersburg=-48.7/5.8/62.1, Saint-Pierre=-45.6/5.7/53.2, Salt Lake City=-37.9/11.6/61.7, San Antonio=-29.8/20.8/70.6, San Diego=-30.9/17.8/67.7, San Francisco=-33.1/14.6/64.0, San Jose=-34.0/16.4/66.7, San José=-30.2/22.6/78.5, San Juan=-21.9/27.2/80.2, San Salvador=-26.2/23.1/72.2, Sana'a=-30.9/20.0/79.6, Santo Domingo=-21.5/25.9/72.6, Sapporo=-39.0/8.9/62.8, Sarajevo=-40.2/10.1/59.3, Saskatoon=-44.7/3.3/54.9, Seattle=-36.1/11.3/61.2, Seoul=-36.1/12.5/61.6, Seville=-30.9/19.2/69.4, Shanghai=-37.0/16.7/68.5, Singapore=-25.4/27.0/74.2, Skopje=-38.3/12.4/67.3, Sochi=-36.0/14.2/61.1, Sofia=-39.6/10.6/61.1, Sokoto=-21.2/28.0/78.0, Split=-35.8/16.1/67.3, St. John's=-47.4/5.0/54.4, St. Louis=-37.2/13.9/62.9, Stockholm=-42.6/6.6/61.0, Surabaya=-22.4/27.1/76.5, Suva=-23.0/25.6/73.9, Suwałki=-44.7/7.2/56.5, Sydney=-33.0/17.7/69.3, Ségou=-23.7/28.0/81.5, Tabora=-24.5/23.0/70.8, Tabriz=-37.6/12.6/60.7, Taipei=-33.3/23.0/73.5, Tallinn=-42.0/6.4/56.7, Tamale=-21.8/27.9/78.6, Tamanrasset=-30.7/21.7/70.6, Tampa=-24.0/22.9/69.4, Tashkent=-32.1/14.8/64.9, Tauranga=-33.9/14.8/64.3, Tbilisi=-39.0/12.9/62.7, Tegucigalpa=-30.8/21.7/69.1, Tehran=-32.8/17.0/70.7, Tel Aviv=-37.7/20.0/68.6, Thessaloniki=-33.5/16.0/66.9, Thiès=-24.5/24.0/74.7, Tijuana=-29.5/17.8/73.0, Timbuktu=-20.6/28.0/82.2, Tirana=-35.2/15.2/63.0, Toamasina=-27.5/23.4/71.8, Tokyo=-36.3/15.4/64.0, Toliara=-28.7/24.1/73.2, Toluca=-37.4/12.4/64.1, Toronto=-38.3/9.4/58.7, Tripoli=-34.5/20.0/67.4, Tromsø=-48.2/2.9/56.1, Tucson=-34.3/20.9/71.5, Tunis=-29.9/18.4/68.5, Ulaanbaatar=-48.6/-0.4/48.5, Upington=-29.3/20.4/70.1, Vaduz=-39.4/10.1/59.5, Valencia=-33.3/18.3/74.7, Valletta=-31.3/18.8/69.3, Vancouver=-42.8/10.4/64.7, Veracruz=-23.3/25.4/74.2, Vienna=-40.5/10.4/63.3, Vientiane=-31.0/25.9/81.4, Villahermosa=-22.6/27.1/76.6, Vilnius=-43.8/6.0/53.4, Virginia Beach=-30.9/15.8/63.9, Vladivostok=-44.6/4.9/53.1, Warsaw=-43.2/8.5/62.5, Washington, D.C.=-40.9/14.6/65.1, Wau=-24.6/27.8/80.2, Wellington=-39.8/12.9/64.5, Whitehorse=-51.3/-0.1/48.0, Wichita=-35.5/13.9/62.6, Willemstad=-21.7/28.0/73.0, Winnipeg=-47.1/3.0/53.4, Wrocław=-40.6/9.6/62.0, Xi'an=-35.2/14.1/66.0, Yakutsk=-61.3/-8.8/40.7, Yangon=-20.9/27.5/75.5, Yaoundé=-27.4/23.8/75.2, Yellowknife=-57.4/-4.3/49.6, Yerevan=-38.5/12.4/64.9, Yinchuan=-40.7/9.0/61.3, Zagreb=-47.3/10.7/60.0, Zanzibar City=-20.1/26.0/76.8, Zürich=-41.4/9.3/60.4, Ürümqi=-43.0/7.4/56.0, İzmir=-32.2/17.9/66.8}"
