import { useEffect, useState } from 'react'

export type Role = 'admin' | 'special' | 'user'

let _cachedRole: Role | null = null
let _promise: Promise<Role> | null = null

function fetchRole(): Promise<Role> {
  if (_promise) return _promise
  _promise = fetch('/api/auth/me')
    .then(r => r.json())
    .then(d => {
      _cachedRole = (d.role as Role) ?? 'user'
      return _cachedRole
    })
    .catch(() => {
      _cachedRole = 'user'
      return 'user' as Role
    })
  return _promise
}

export function useRole(): Role | null {
  const [role, setRole] = useState<Role | null>(_cachedRole)

  useEffect(() => {
    if (_cachedRole) {
      setRole(_cachedRole)
      return
    }
    fetchRole().then(setRole)
  }, [])

  return role
}

export function canViewPortfolio(role: Role | null): boolean {
  return role === 'admin' || role === 'special'
}
