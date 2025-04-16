import React from 'react';
import { City, RouteData } from '../types';
import { MapPin, ArrowRight } from 'lucide-react';

interface CityCardProps {
  city: City;
  routes: RouteData[];
  expanded?: boolean;
  onClick?: () => void;
}

export function CityCard({ city, routes, expanded = false, onClick }: CityCardProps) {
  const getStatusCount = (level: 'High' | 'Medium' | 'Low') => {
    return routes.filter(route => route.congestionLevel === level).length;
  };

  const cardClasses = expanded
    ? 'w-full'
    : 'transform transition-transform duration-300 hover:scale-105 cursor-pointer';

  return (
    <div
      className={`bg-white rounded-xl shadow-lg overflow-hidden ${cardClasses}`}
      onClick={!expanded ? onClick : undefined}
    >
      <div className="p-6">
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center gap-2">
            <MapPin className="w-6 h-6 text-indigo-600" />
            <h2 className="text-2xl font-bold text-gray-900">{city.name}</h2>
          </div>
          {!expanded && (
            <ArrowRight className="w-5 h-5 text-gray-400" />
          )}
        </div>

        <div className="space-y-4">
          <div className="grid grid-cols-3 gap-4">
            <div className="bg-red-50 p-3 rounded-lg">
              <div className="text-2xl font-bold text-red-700">{getStatusCount('High')}</div>
              <div className="text-sm text-red-600">High</div>
            </div>
            <div className="bg-yellow-50 p-3 rounded-lg">
              <div className="text-2xl font-bold text-yellow-700">{getStatusCount('Medium')}</div>
              <div className="text-sm text-yellow-600">Medium</div>
            </div>
            <div className="bg-green-50 p-3 rounded-lg">
              <div className="text-2xl font-bold text-green-700">{getStatusCount('Low')}</div>
              <div className="text-sm text-green-600">Low</div>
            </div>
          </div>

          {expanded && (
            <div className="mt-6">
              <h3 className="text-lg font-semibold text-gray-900 mb-4">Route Details</h3>
              <div className="space-y-3">
                {routes.map(route => (
                  <div
                    key={route.id}
                    className="flex items-center justify-between p-4 bg-gray-50 rounded-lg"
                  >
                    <span className="font-medium text-gray-900">{route.name}</span>
                    <span className={`px-3 py-1 rounded-full text-sm font-medium ${
                      route.congestionLevel === 'High' ? 'bg-red-100 text-red-800' :
                      route.congestionLevel === 'Medium' ? 'bg-yellow-100 text-yellow-800' :
                      'bg-green-100 text-green-800'
                    }`}>
                      {route.congestionLevel}
                    </span>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}