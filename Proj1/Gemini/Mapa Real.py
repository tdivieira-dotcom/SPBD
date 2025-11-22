# --------------------------------------------------------------------------------------------
# | Local (Landmark)           Latitude, Longitude (Aprox)  Célula da Grelha (Linha, Coluna) |
# | Times Square (Midtown)     40.7580, -73.9855            (160, 155)                       |
# | Central Park (Centro)      40.7829, -73.9654            (154, 158)                       |
# | Wall Street (Downtown)     40.7070, -74.0100            (171, 151)                       |
# | Aeroporto JFK              40.6413, -73.7781            (186, 190)                       |
# | Aeroporto LaGuardia        40.7769, -73.8740            (155, 174)                       |
# | Barclays Center (Brooklyn) 40.6820, -73.9750            (177, 157)                       |
# --------------------------------------------------------------------------------------------


# ---------------------------------------------------------
# FERRAMENTA DE INVESTIGAÇÃO: Grelha -> Mapa Real
# ---------------------------------------------------------
def grid_to_latlon(row, col):
    # Inverter a fórmula: 
    # row = (MAX_LAT - lat) / LAT_DELTA  --> lat = MAX_LAT - (row * LAT_DELTA)
    lat = MAX_LAT - (row * LAT_DELTA)
    lon = MIN_LON + (col * LON_DELTA)
    return lat, lon

def check_place(row, col):
    lat, lon = grid_to_latlon(row, col)
    print(f"Célula ({row}, {col}) corresponde aprox. a:")
    print(f"Latitude: {lat:.4f}, Longitude: {lon:.4f}")
    print(f"Google Maps: https://www.google.com/maps/search/?api=1&query={lat},{lon}")

# Testa com o teu Top 1 da tabela anterior (ex: 176, 185)
print("--- A investigar o Mistério da Célula (176, 185) ---")
check_place(176, 185)
