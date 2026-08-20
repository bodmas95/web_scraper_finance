import { useEffect, useState } from 'react'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { Label } from '@/components/ui/label'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { fetchRegions, addCompany } from '@/api/client'
import type { Region, Country, Company } from '@/types'
import { Plus } from 'lucide-react'

interface RegionSelectorProps {
  onSelectionChange: (selection: {
    region: Region
    country: Country
    company: Company
  }) => void
}

export function RegionSelector({ onSelectionChange }: RegionSelectorProps) {
  const [regions, setRegions] = useState<Region[]>([])
  const [selectedRegion, setSelectedRegion] = useState<Region | null>(null)
  const [selectedCountry, setSelectedCountry] = useState<Country | null>(null)
  const [selectedCompany, setSelectedCompany] = useState<Company | null>(null)
  const [showAddCompany, setShowAddCompany] = useState(false)
  const [newCompanyName, setNewCompanyName] = useState('')
  const [newCurrency] = useState('USD')

  useEffect(() => {
    fetchRegions().then(setRegions)
  }, [])

  const handleRegionChange = (code: string | null) => {
    if (!code) return
    const region = regions.find((r) => r.region_code === code)
    setSelectedRegion(region ?? null)
    setSelectedCountry(null)
    setSelectedCompany(null)

    if (region && region.countries.length === 1) {
      setSelectedCountry(region.countries[0])
    }
  }

  const handleCountryChange = (code: string | null) => {
    if (!code) return
    const country = selectedRegion?.countries.find(
      (c) => c.country_code === code
    )
    setSelectedCountry(country ?? null)
    setSelectedCompany(null)
  }

  const handleCompanyChange = (id: string | null) => {
    if (!id) return
    const company = selectedCountry?.companies.find(
      (c) => c.company_id === id
    )
    setSelectedCompany(company ?? null)
    if (company && selectedRegion && selectedCountry) {
      onSelectionChange({
        region: selectedRegion,
        country: selectedCountry,
        company,
      })
    }
  }

  const handleAddCompany = async () => {
    if (!selectedRegion || !selectedCountry || !newCompanyName.trim()) return
    const currencyMap: Record<string, string> = {
      US: 'USD',
      HK: 'HKD',
      FR: 'EUR',
      IT: 'EUR',
    }
    const currency = currencyMap[selectedCountry.country_code] ?? newCurrency

    await addCompany(
      selectedRegion.region_code,
      selectedCountry.country_code,
      newCompanyName.trim(),
      currency
    )
    const refreshed = await fetchRegions()
    setRegions(refreshed)
    const updatedRegion = refreshed.find(
      (r) => r.region_code === selectedRegion.region_code
    )
    setSelectedRegion(updatedRegion ?? null)
    const updatedCountry = updatedRegion?.countries.find(
      (c) => c.country_code === selectedCountry.country_code
    )
    setSelectedCountry(updatedCountry ?? null)
    setNewCompanyName('')
    setShowAddCompany(false)
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Select Company</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
          <div className="space-y-2">
            <Label>Region</Label>
            <Select
              value={selectedRegion?.region_code ?? ''}
              onValueChange={handleRegionChange}
            >
              <SelectTrigger className="w-full">
                <SelectValue placeholder="Select region" />
              </SelectTrigger>
              <SelectContent>
                {regions.map((r) => (
                  <SelectItem key={r.region_code} value={r.region_code}>
                    {r.region_code} - {r.region_name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <div className="space-y-2">
            <Label>Country</Label>
            <Select
              value={selectedCountry?.country_code ?? ''}
              onValueChange={handleCountryChange}
              disabled={!selectedRegion}
            >
              <SelectTrigger className="w-full">
                <SelectValue placeholder="Select country" />
              </SelectTrigger>
              <SelectContent>
                {selectedRegion?.countries.map((c) => (
                  <SelectItem key={c.country_code} value={c.country_code}>
                    {c.country_code} - {c.country_name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <div className="space-y-2">
            <Label>Company</Label>
            <div className="flex gap-2">
              <Select
                value={selectedCompany?.company_id ?? ''}
                onValueChange={handleCompanyChange}
                disabled={!selectedCountry}
              >
                <SelectTrigger className="w-full">
                  <SelectValue placeholder="Select company" />
                </SelectTrigger>
                <SelectContent>
                  {selectedCountry?.companies.map((c) => (
                    <SelectItem key={c.company_id} value={c.company_id}>
                      {c.company_name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              {selectedCountry && (
                <Button
                  variant="outline"
                  size="icon"
                  onClick={() => setShowAddCompany(!showAddCompany)}
                >
                  <Plus className="h-4 w-4" />
                </Button>
              )}
            </div>
          </div>
        </div>

        {showAddCompany && (
          <div className="mt-4 flex items-end gap-3 rounded-lg border bg-muted/50 p-4">
            <div className="flex-1 space-y-2">
              <Label>Company Name</Label>
              <input
                className="flex h-9 w-full rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-sm placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
                value={newCompanyName}
                onChange={(e) => setNewCompanyName(e.target.value)}
                placeholder="Enter company name"
              />
            </div>
            <Button onClick={handleAddCompany} disabled={!newCompanyName.trim()}>
              Add Company
            </Button>
          </div>
        )}
      </CardContent>
    </Card>
  )
}
