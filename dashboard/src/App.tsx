import { BrowserRouter, Navigate, NavLink, Route, Routes } from "react-router-dom";
import FraudOverview from "./pages/FraudOverview";
import FraudDetail from "./pages/FraudDetail";
import QualityOverview from "./pages/QualityOverview";
import QualityDetail from "./pages/QualityDetail";

export default function App() {
  return (
    <BrowserRouter>
      <header className="app-header">
        <h1>Fraud Detection &amp; Quality Analysis</h1>
        <nav>
          <NavLink to="/fraud">Fraud Overview</NavLink>
          <NavLink to="/quality">Quality Overview</NavLink>
        </nav>
      </header>

      <main className="app-main">
        <Routes>
          <Route path="/" element={<Navigate to="/fraud" replace />} />
          <Route path="/fraud" element={<FraudOverview />} />
          <Route path="/fraud/:claimId" element={<FraudDetail />} />
          <Route path="/quality" element={<QualityOverview />} />
          <Route path="/quality/:manufacturerId" element={<QualityDetail />} />
        </Routes>
      </main>
    </BrowserRouter>
  );
}
