#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 20 12:20:40 2025

@author: mutua
"""

# name_generator.py
from faker import Faker
import random
import gender_guesser.detector as gender

fake = Faker("en_US")
detector = gender.Detector()

# ----------------------------------
# Define Kenyan names by region
# ----------------------------------
central_names = ["Kamau", "Mwangi", "Wanjiku", "Wambui", "Njeri", "Kariuki", "Kimani", "Nduta", "Wairimu", "Mugo"]
luo_names = ["Otieno", "Omondi", "Ouma", "Achieng", "Owino", "Okoth", "Were", "Odhiambo", "Atieno"]
kalenjin_names = ["Kiptoo", "Koech", "Cheruiyot", "Chebet", "Kibet", "Rono", "Kiplangat", "Cherono", "Chepkoech", "Kipruto"]
luhya_names = ["Wekesa", "Wafula", "Barasa", "Mukhwana", "Nabwire", "Mukasa", "Wanjala", "Nangila", "Mabonga"]
kamba_names = ["Mutua", "Mutiso", "Muema", "Ndunda", "Kioko", "Mumo", "Kalonzo", "Mumbua", "Mwaniki", "Mwikali"]
coastal_names = ["Mwinyi", "Bakari", "Salim", "Ali", "Omar", "Mwajuma", "Kassim", "Amina", "Baya", "Masha", "Chirau", "Hassan"]
somali_names = ["Hussein", "Mohamed", "Abdi", "Yusuf", "Farah", "Ismail", "Ahmed", "Dahir", "Ali", "Warsame", "Abdullahi"]
maasai_names = ["Naisiae", "Naserian", "Sankale", "Koitamet", "Ole", "Nchoe", "Ntasia", "Saruni", "Ntutu", "Tepesua"]
kisii_names = ["Moraa", "Ongeri", "Nyaboke", "Onsongo", "Mogaka", "Nyamweya", "Nyanchoka", "Nyakundi"]
meru_embu_names = ["Muthomi", "Muriuki", "Mutembei", "Nthiga", "Kaberia", "Mwenda", "Naitore", "Kirimi"]
turkana_pokot_names = ["Ekal", "Lomuria", "Nakua", "Ebei", "Ng'asike", "Lopokoyit", "Longor", "Apalot"]
borana_names = ["Galgallo", "Adan", "Dida", "Guyo", "Wario", "Jarso"]

# ----------------------------------
# County → name mapping
# ----------------------------------
county_name_mapping = {
    "Nairobi": central_names + luhya_names + luo_names + kamba_names + kalenjin_names + kisii_names + coastal_names + somali_names + maasai_names + meru_embu_names,
    "Mombasa": coastal_names + somali_names + central_names + kamba_names + luhya_names + luo_names,
    "Kisumu": luo_names + luhya_names + kisii_names + kamba_names + central_names,
    "Kiambu": central_names,
    "Murang’a": central_names,
    "Nyeri": central_names,
    "Kirinyaga": central_names,
    "Machakos": kamba_names,
    "Makueni": kamba_names,
    "Kitui": kamba_names,
    "Meru": meru_embu_names,
    "Embu": meru_embu_names,
    "Kakamega": luhya_names,
    "Vihiga": luhya_names,
    "Bungoma": luhya_names,
    "Busia": luhya_names,
    "Homa Bay": luo_names,
    "Siaya": luo_names,
    "Migori": luo_names,
    "Kisii": kisii_names,
    "Narok": maasai_names,
    "Kajiado": maasai_names,
    "Kwale": coastal_names,
    "Kilifi": coastal_names,
    "Tana River": coastal_names,
    "Lamu": coastal_names,
    "Garissa": somali_names,
    "Wajir": somali_names,
    "Mandera": somali_names,
    "Marsabit": borana_names,
    "Isiolo": borana_names,
    "Uasin Gishu": kalenjin_names,
    "Kericho": kalenjin_names,
    "Bomet": kalenjin_names,
    "Elgeyo Marakwet": kalenjin_names,
    "Turkana": turkana_pokot_names,
    "West Pokot": turkana_pokot_names,
    "Baringo": kalenjin_names,
    "Trans Nzoia": kalenjin_names,
    "Laikipia": central_names + kalenjin_names,
    "Nakuru": central_names + kalenjin_names,
    "Samburu": maasai_names
}

# ----------------------------------
# Function to generate a name + gender + age
# ----------------------------------
def generate_county_based_name(county: str) -> dict:
    """Generate a realistic Kenyan name, gender, and age given a county."""
    first = fake.first_name()
    possible_surnames = county_name_mapping.get(county, central_names + luo_names + kamba_names)
    last = random.choice(possible_surnames)
    gender_guess = detector.get_gender(first)
    
    # Normalize gender labels
    if gender_guess in ["male", "mostly_male"]:
        gender_label = "Male"
    elif gender_guess in ["female", "mostly_female"]:
        gender_label = "Female"
    else:
        gender_label = "Unknown"

    # Randomly simulate age
    # Most are adults (18–60), some minors (10–17), some missing
    age_distribution = random.choices(
        population=["adult", "minor", "missing"],
        weights=[0.75, 0.15, 0.10],
        k=1
    )[0]

    if age_distribution == "adult":
        age = random.randint(18, 80)
    elif age_distribution == "minor":
        age = random.randint(10, 17)
    else:
        age = None  # missing age

    return {
        "full_name": f"{first} {last}",
        "first_name": first,
        "last_name": last,
        "gender": gender_label,
        "age": age,
        "county": county
    }

# Example usage
if __name__ == "__main__":
    for _ in range(10):
        example = generate_county_based_name(random.choice(list(county_name_mapping.keys())))
        print(example)