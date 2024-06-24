#!/usr/bin/python3
import pandas as pd
import random


class ResidentialRealEstate:
    def __init__(self, address, city, state, zip_code, square_feet, bedrooms, bathrooms, price, year_built, lot_size):
        self.address = address
        self.city = city
        self.state = state
        self.zip_code = zip_code
        self.square_feet = square_feet
        self.bedrooms = bedrooms
        self.bathrooms = bathrooms
        self.price = price
        self.year_built = year_built
        self.lot_size = lot_size

    def to_dict(self):
        return {
            "address": self.address,
            "city": self.city,
            "state": self.state,
            "zip_code": self.zip_code,
            "square_feet": self.square_feet,
            "bedrooms": self.bedrooms,
            "bathrooms": self.bathrooms,
            "price": self.price,
            "year_built": self.year_built,
            "lot_size": self.lot_size,
        }


def generate_real_estate_data(num_samples=10000):
    data = []
    states = ['CA', 'TX', 'FL', 'NY', 'IL', 'PA', 'OH', 'GA', 'NC', 'MI']
    for _ in range(num_samples):
        address = f"{random.randint(1, 9999)} Main St"
        city = f"City{random.randint(1, 1000)}"
        state = random.choice(states)
        zip_code = f"{random.randint(10000, 99999)}"
        square_feet = random.randint(1000, 5000)
        bedrooms = random.randint(1, 7)
        bathrooms = random.randint(1, 5)
        price = random.randint(50000, 1000000)
        year_built = random.randint(1900, 2023)
        lot_size = round(random.uniform(0.1, 5.0), 2)

        real_estate = ResidentialRealEstate(
            address, city, state, zip_code, square_feet, bedrooms, bathrooms, price, year_built, lot_size
        )
        data.append(real_estate.to_dict())

    return data


def save_to_parquet(data, file_name="real_estate_data.parquet"):
    df = pd.DataFrame(data)
    df.to_parquet(file_name, index=False)


# Generate data
real_estate_data = generate_real_estate_data()

# Save to Parquet file
save_to_parquet(real_estate_data)
