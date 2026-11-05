import { useQuery } from "@tanstack/react-query";
import { supabase } from "@/integrations/supabase/client";

export function useUserTables() {
  return useQuery({
    queryKey: ["user_tables"],
    // Rows come through the database's secure_tables(): row rules and column masks are applied server-side.
    queryFn: async () => {
      const { data, error } = await supabase.rpc("secure_tables");
      if (error) throw error;
      return data ?? [];
    },
  });
}
