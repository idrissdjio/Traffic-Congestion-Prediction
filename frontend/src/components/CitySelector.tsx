import React from 'react';
import { MapIcon } from 'lucide-react';
import { City } from '../types';

interface CitySelectorProps {
  cities: City[];
  selectedCity: City;
  onCityChange: (city: City) => void;
}

export function CitySelector({ cities, selectedCity, onCityChange }: CitySelectorProps) {
  return (
    <div className="absolute top-4 left-4 bg-white rounded-lg shadow-lg p-4 z-10">
      <div className="flex items-center gap-2 mb-3">
        <MapIcon className="w-5 h-5 text-blue-600" />
        <h2 className="text-lg font-semibold text-gray-800">Select City</h2>
      </div>
      <div className="flex flex-col gap-2">
        {cities.map((city) => (
          <button
            key={city.id}
            onClick={() => onCityChange(city)}
            className={`px-4 py-2 rounded-md text-left transition-colors ${
              selectedCity.id === city.id
                ? 'bg-blue-600 text-white'
                : 'bg-gray-100 hover:bg-gray-200 text-gray-800'
            }`}
          >
            {city.name}
          </button>
        ))}
      </div>
    </div>
  );
}