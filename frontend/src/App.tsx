import React, { useState, useEffect } from 'react';
import { cities, routes } from './data/sampleData';
import { CityCard } from './components/CityCard';
import { Clock } from 'lucide-react';
import type { City, RouteData } from './types';

function App() {
  const [selectedCity, setSelectedCity] = useState<City | null>(null);
  const [cityRoutes, setCityRoutes] = useState<Record<string, RouteData[]>>(routes);
  const [lastUpdate, setLastUpdate] = useState<Date>(new Date());

  // Function to randomly update congestion levels
  const updateCongestionLevels = () => {
    const congestionLevels = ['Low', 'Medium', 'High'];
    const updatedRoutes = { ...cityRoutes };
    
    Object.keys(updatedRoutes).forEach(cityId => {
      updatedRoutes[cityId] = updatedRoutes[cityId].map(route => ({
        ...route,
        congestionLevel: congestionLevels[Math.floor(Math.random() * congestionLevels.length)] as 'Low' | 'Medium' | 'High'
      }));
    });

    setCityRoutes(updatedRoutes);
    setLastUpdate(new Date());
  };

  // Update congestion levels every 30 seconds
  useEffect(() => {
    const interval = setInterval(updateCongestionLevels, 5000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 to-indigo-50 p-8">
      <header className="max-w-7xl mx-auto mb-8">
        <h1 className="text-4xl font-bold text-gray-900 mb-2">Traffic Congestion Monitor</h1>
        <div className="flex items-center gap-2 text-gray-600">
          <Clock className="w-5 h-5" />
          <span>Last updated: {lastUpdate.toLocaleTimeString()}</span>
        </div>
      </header>

      <main className="max-w-7xl mx-auto">
        {selectedCity ? (
          <div>
            <button
              onClick={() => setSelectedCity(null)}
              className="mb-6 px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 transition-colors"
            >
              ← Back to All Cities
            </button>
            <CityCard
              city={selectedCity}
              routes={cityRoutes[selectedCity.id]}
              expanded={true}
            />
          </div>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {cities.map(city => (
              <CityCard
                key={city.id}
                city={city}
                routes={cityRoutes[city.id]}
                onClick={() => setSelectedCity(city)}
              />
            ))}
          </div>
        )}
      </main>
    </div>
  );
}

export default App