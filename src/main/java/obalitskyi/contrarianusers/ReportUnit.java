package obalitskyi.contrarianusers;

import java.io.Serializable;


public class ReportUnit implements Serializable {
    private String title;
    private Integer year;
    private String dateOfRating;

    public ReportUnit(String title, Integer year, String dateOfRating) {
        this.title = title;
        this.year = year;
        this.dateOfRating = dateOfRating;
    }

    public ReportUnit() {
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    public String getDateOfRating() {
        return dateOfRating;
    }

    public void setDateOfRating(String dateOfRating) {
        this.dateOfRating = dateOfRating;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ReportUnit that = (ReportUnit) o;

        if (title != null ? !title.equals(that.title) : that.title != null) return false;
        if (year != null ? !year.equals(that.year) : that.year != null) return false;
        return dateOfRating != null ? dateOfRating.equals(that.dateOfRating) : that.dateOfRating == null;
    }

    @Override
    public int hashCode() {
        int result = title != null ? title.hashCode() : 0;
        result = 31 * result + (year != null ? year.hashCode() : 0);
        result = 31 * result + (dateOfRating != null ? dateOfRating.hashCode() : 0);
        return result;
    }
}
