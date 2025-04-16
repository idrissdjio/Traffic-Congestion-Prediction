export type CongestionLevel = 'Low' | 'Medium' | 'High';

export interface RouteData {
  id: string;
  name: string;
  coordinates: [number, number][];
  congestionLevel: CongestionLevel;
}

export interface City {
  id: string;
  name: string;
  coordinates: [number, number];
  zoom: number;
}